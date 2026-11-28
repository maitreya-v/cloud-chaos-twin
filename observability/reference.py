"""Render a temporary, non-tracing request ledger in front of one Boutique service.

Admin is loopback-only. No request/response bodies, cookies, addresses or
credentials are logged. Service ports stay unchanged; only targetPort changes.
"""
PORTS={"frontend":8080,"adservice":9555,"cartservice":7070,"checkoutservice":5050,
       "currencyservice":7000,"emailservice":8080,"paymentservice":50051,
       "productcatalogservice":3550,"recommendationservice":8080,"shippingservice":50051}
DATA_PORT=18080
ADMIN_PORT=15000


def configuration(service):
    if service not in PORTS:
        raise ValueError("unknown reference service")
    cluster={"name":"application","type":"STATIC","connect_timeout":"1s",
             "load_assignment":{"cluster_name":"application","endpoints":[{"lb_endpoints":[{
                 "endpoint":{"address":{"socket_address":{"address":"127.0.0.1","port_value":PORTS[service]}}}}]}]}}
    if service!="frontend":
        cluster["typed_extension_protocol_options"]={"envoy.extensions.upstreams.http.v3.HttpProtocolOptions":{
            "@type":"type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions",
            "explicit_http_config":{"http2_protocol_options":{}}}}
    ledger={"cct_reference":"v1","service":service,"event":"%ACCESS_LOG_TYPE%","start_epoch_s":"%START_TIME(%s.%6f)%",
            "duration_us":"%COMMON_DURATION(DS_RX_BEG:DS_TX_END:us)%",
            "upstream_end_us":"%COMMON_DURATION(DS_RX_BEG:US_RX_END:us)%",
            "method":"%REQ(:METHOD)%","path":"%REQ(:PATH)%","traceparent":"%REQ(TRACEPARENT)%",
            "http_status":"%RESPONSE_CODE%","grpc_status":"%GRPC_STATUS(NUMBER)%",
            "flags":"%RESPONSE_FLAGS%","upstream_attempts":"%UPSTREAM_REQUEST_ATTEMPT_COUNT%"}
    hcm={"@type":"type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager",
         "stat_prefix":"cct_reference","codec_type":"AUTO","stream_idle_timeout":"0s",
         "generate_request_id":False,
         "access_log_options":{"flush_access_log_on_new_request":True},
         "route_config":{"name":"application","virtual_hosts":[{"name":"application","domains":["*"],
             "routes":[{"match":{"prefix":"/"},"route":{"cluster":"application","timeout":"0s"}}]}]},
         "http_filters":[{"name":"envoy.filters.http.router","typed_config":{
             "@type":"type.googleapis.com/envoy.extensions.filters.http.router.v3.Router"}}],
         "access_log":[{"name":"envoy.access_loggers.stdout","typed_config":{
             "@type":"type.googleapis.com/envoy.extensions.access_loggers.stream.v3.StdoutAccessLog",
             "log_format":{"json_format":ledger}}}]}
    return {"static_resources":{"listeners":[{"name":"reference","address":{"socket_address":{
                "address":"0.0.0.0","port_value":DATA_PORT}},"filter_chains":[{"filters":[{
                "name":"envoy.filters.network.http_connection_manager","typed_config":hcm}]}]}],"clusters":[cluster]},
            "admin":{"address":{"socket_address":{"address":"127.0.0.1","port_value":ADMIN_PORT}}}}


def sidecar(image, config_name):
    if "@sha256:" not in image:
        raise ValueError("use the verified immutable image")
    return {"name":"cct-reference","image":image,"imagePullPolicy":"Never",
            "args":["-c","/etc/cct-reference/envoy.json","--concurrency","1","--disable-hot-restart","--log-level","warning",
                    "--file-flush-interval-msec","50"],
            "resources":{"requests":{"cpu":"10m","memory":"24Mi"},"limits":{"cpu":"250m","memory":"96Mi"}},
            "securityContext":{"runAsUser":10001,"runAsGroup":10001,"runAsNonRoot":True,
                               "allowPrivilegeEscalation":False,"readOnlyRootFilesystem":True,
                               "capabilities":{"drop":["ALL"]},"seccompProfile":{"type":"RuntimeDefault"}},
            "ports":[{"name":"cct-reference","containerPort":DATA_PORT}],
            "readinessProbe":{"tcpSocket":{"port":DATA_PORT},"periodSeconds":2,"timeoutSeconds":1},
            "startupProbe":{"tcpSocket":{"port":DATA_PORT},"periodSeconds":1,"failureThreshold":45},
            "volumeMounts":[{"name":"cct-reference","mountPath":"/etc/cct-reference","readOnly":True}]}
