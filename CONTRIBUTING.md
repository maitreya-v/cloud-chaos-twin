# Contributing

Start from the current `main` branch and use a short feature branch such as `feature/trace-collection`. Keep one deliverable per pull request. Include what changed, how to run it, and evidence that it works. Another teammate should review changes before merge.

Run these checks from the repository root:

```sh
python3 -m cct check
python3 -m unittest discover -s tests -v
python3 -m cct demo
```

Update the relevant component README and task status when a deliverable changes. Discuss schema changes with both the producer and consumer before changing them. Include a schema version on every record.

Do not commit credentials, kubeconfig files, student IDs, large telemetry captures, or generated results. Small synthetic fixtures belong in `data/samples/`. Confirm the license of upstream code before copying it; retain required attribution. A project-wide license has not yet been selected by the team.

Use two short technical syncs and one integrated demonstration each week. Reserve experiment sessions so simultaneous deployments do not invalidate data collection. Every live run should identify its code/configuration revision and actual fault interval.
