// Run inside a built Node service image. Export one real SDK span to a local
// gRPC receiver so SDK/exporter serialization failures fail the image preflight.
'use strict';

const grpc = require('@grpc/grpc-js');
const { trace } = require('@opentelemetry/api');
const { NodeSDK } = require('@opentelemetry/sdk-node');
const { OTLPTraceExporter } = require('@opentelemetry/exporter-trace-otlp-grpc');

async function main() {
  const server = new grpc.Server();
  let received = false;
  server.addService({ export: {
    path: '/opentelemetry.proto.collector.trace.v1.TraceService/Export',
    requestStream: false, responseStream: false,
    requestDeserialize: value => value,
    responseSerialize: () => Buffer.alloc(0),
  } }, { export: (call, callback) => {
    received = call.request.includes(Buffer.from('cct-exporter-preflight'));
    callback(null, {});
  } });
  try {
    const port = await new Promise((resolve, reject) => server.bindAsync(
      '127.0.0.1:0', grpc.ServerCredentials.createInsecure(),
      (error, value) => error ? reject(error) : resolve(value)));
    const sdk = new NodeSDK({ autoDetectResources: false,
      traceExporter: new OTLPTraceExporter({ url: `http://127.0.0.1:${port}` }) });
    sdk.start();
    trace.getTracer('cct-preflight').startSpan('cct-exporter-preflight').end();
    await sdk.shutdown();
    if (!received) throw new Error('SDK span was not serialized and received over OTLP/gRPC');
    console.log('Node SDK/exporter OTLP/gRPC preflight passed');
  } finally {
    server.forceShutdown();
  }
}

setTimeout(() => { console.error('Exporter preflight timed out'); process.exit(1); }, 15000).unref();
main().catch(error => { console.error(error.message); process.exitCode = 1; });
