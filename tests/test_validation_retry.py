import json
import copy
from pathlib import Path
import tempfile
import unittest

from scripts.validate_cct10 import retained_training
from observability.reference_control import assert_same_templates


class RetainedTrainingTests(unittest.TestCase):
    def archive(self,root,**changes):
        parent=Path(root)/'campaign';source=parent/'runs'/'healthy'
        source.mkdir(parents=True)
        receipt={'passed':True,'mode':'healthy','fault':{},'coverage_review':{'verified':True}}
        receipt.update(changes)
        (source/'receipt.json').write_text(json.dumps(receipt))
        (parent/'receipt.json').write_text(json.dumps({'restoration_errors':[]}))
        (parent/'reference-state.json').write_text(json.dumps({'image':'pinned-image'}))
        return parent,source

    def test_failed_or_reserved_data_cannot_be_reused_for_training(self):
        cases=({'passed':False},{'mode':'fault'},{'fault':{'requested_at':'recorded'}},
               {'coverage_review':{'verified':False}})
        for changes in cases:
            with self.subTest(changes=changes),tempfile.TemporaryDirectory() as root:
                _,source=self.archive(root,**changes)
                with self.assertRaisesRegex(ValueError,'passed healthy capture'):
                    retained_training(source,'pinned-image')

    def test_unclean_restoration_or_changed_reference_image_rejects_reuse(self):
        with tempfile.TemporaryDirectory() as root:
            parent,source=self.archive(root)
            with self.assertRaisesRegex(ValueError,'different reference image'):
                retained_training(source,'other-image')
            (parent/'receipt.json').write_text(json.dumps({'restoration_errors':['not restored']}))
            with self.assertRaisesRegex(ValueError,'clean restoration'):
                retained_training(source,'pinned-image')

    def test_receipt_metadata_alone_cannot_replace_raw_reconciliation(self):
        with tempfile.TemporaryDirectory() as root:
            _,source=self.archive(root)
            with self.assertRaises(FileNotFoundError):retained_training(source,'pinned-image')

    def test_training_environment_cannot_silently_change_on_retry(self):
        before={'frontend':{'spec':{'template':{'spec':{'containers':[{'image':'immutable-a','resources':{'cpu':'200m'}}]}}}}}
        assert_same_templates(copy.deepcopy(before),before)
        for key,value in (('image','immutable-b'),('resources',{'cpu':'400m'})):
            current=copy.deepcopy(before)
            current['frontend']['spec']['template']['spec']['containers'][0][key]=value
            with self.assertRaisesRegex(ValueError,'template differs'):assert_same_templates(current,before)
