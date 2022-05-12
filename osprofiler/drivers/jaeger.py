# Copyright 2018 Fujitsu Ltd.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import collections
import datetime
import time
from urllib import parse as parser

from oslo_config import cfg
from oslo_serialization import jsonutils

from osprofiler import _utils as utils
from osprofiler.drivers import base
from osprofiler import exc

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.instrumentation.utils import _start_internal_or_server_span

class Jaeger(base.Driver):
    def __init__(self, connection_str, project=None, service=None, host=None,
                 conf=cfg.CONF, **kwargs):
        """Jaeger driver for OSProfiler."""

        super(Jaeger, self).__init__(connection_str, project=project,
                                     service=service, host=host,
                                     conf=conf, **kwargs)
        # try:
        #     import jaeger_client
        #     self.jaeger_client = jaeger_client
        # except ImportError:
        #     raise exc.CommandError(
        #         "To use OSProfiler with Uber Jaeger tracer, "
        #         "please install `jaeger-client` library. "
        #         "To install with pip:\n `pip install jaeger-client`."
        #     )

        # parsed_url = parser.urlparse(connection_str)
        # cfg = {
        #     "local_agent": {
        #         "reporting_host": parsed_url.hostname,
        #         "reporting_port": parsed_url.port,
        #     }
        # }

        # # Initialize tracer for each profiler
        service_name = "{}-{}".format(project, service)
        # config = jaeger_client.Config(cfg, service_name=service_name)
        # self.tracer = config.initialize_tracer()

        trace.set_tracer_provider(
            TracerProvider(
                resource=Resource.create({SERVICE_NAME: service_name})
            )
        )

        jaeger_exporter = JaegerExporter(
            agent_host_name="192.168.56.1",
            agent_port=6831,
        )

        trace.get_tracer_provider().add_span_processor(
            BatchSpanProcessor(jaeger_exporter)
        )

        self.tracer = trace.get_tracer(__name__)

        self.spans = collections.deque()

    @classmethod
    def get_name(cls):
        return "jaeger"

    def notify(self, payload):
        if payload["name"].endswith("start"):
            timestamp = datetime.datetime.strptime(payload["timestamp"],
                                                   "%Y-%m-%dT%H:%M:%S.%f")
            epoch = datetime.datetime.utcfromtimestamp(0)
            start_time = (timestamp - epoch).total_seconds()

            # Create parent span
            # child_of = self.jaeger_client.SpanContext(
            #     trace_id=utils.shorten_id(payload["base_id"]),
            #     span_id=utils.shorten_id(payload["parent_id"]),
            #     parent_id=None,
            #     flags=self.jaeger_client.span.SAMPLED_FLAG
            # )

            parent_span = None
            if payload["parent_id"] is not None:
                trace_id = payload["base_id"]
                span_id = payload["parent_id"]

                print("trace id: {}, span_id: {}".format(trace_id, span_id))

                parent_span = trace.NonRecordingSpan(
                    trace.SpanContext(
                        trace_id=trace_id,
                        span_id=span_id,
                        is_remote=True,
                        trace_flags=trace.TraceFlags(trace.TraceFlags.SAMPLED),
                    )
                )


            ctx = trace.set_span_in_context(parent_span)            

            # Create Jaeger Tracing span
            print("starting span!")
            span = self.tracer.start_span(
                name=payload["name"].rstrip("-start"),
                context=ctx,
                # child_of=child_of,
                attributes=self.create_span_tags(payload),
                #start_time=start_time
            )

            # span, token = _start_internal_or_server_span(
            #     tracer=self.tracer,
            #     span_name=payload["name"].rstrip("-start"),
            #     # start_time=flask_request_environ.get(_ENVIRON_STARTTIME_KEY),
            #     # context_carrier=flask_request_environ,
            #     # context_getter=otel_wsgi.wsgi_getter,
            # )


            activation = trace.use_span(span, end_on_exit=True)
            activation.__enter__()  # pylint: disable=E1101

            self.spans.append(activation)
            print("\n##### span: {}".format(span))
            print("\n span_id {}".format(span.context.span_id))

            current_span_id = span.get_span_context().span_id
            
            if parent_span is None:
                # return trace id and span id
                trace_id = span.get_span_context().trace_id
                return [trace_id, current_span_id]

            return [current_span_id]
        else:
            activation = self.spans.pop()

            # # Store result of db call and function call
            # for call in ("db", "function"):
            #     if payload.get("info", {}).get(call) is not None:
            #         span.set_tag("result", payload["info"][call]["result"])

            # # Span error tag and log
            # if payload["info"].get("etype") is not None:
            #     span.set_tag("error", True)
            #     span.log_kv({"error.kind": payload["info"]["etype"]})
            #     span.log_kv({"message": payload["info"]["message"]})

            print("stopping span!")
            activation.__exit__(None, None, None)

    def get_report(self, base_id):
        """Please use Jaeger Tracing UI for this task."""
        return self._parse_results()

    def list_traces(self, fields=None):
        """Please use Jaeger Tracing UI for this task."""
        return []

    def list_error_traces(self):
        """Please use Jaeger Tracing UI for this task."""
        return []

    def create_span_tags(self, payload):
        """Create tags for OpenTracing span.

        :param info: Information from OSProfiler trace.
        :returns tags: A dictionary contains standard tags
                       from OpenTracing sematic conventions,
                       and some other custom tags related to http, db calls.
        """
        tags = {}
        info = payload["info"]

        if info.get("db"):
            # DB calls
            tags["db.statement"] = info["db"]["statement"]
            tags["db.params"] = jsonutils.dumps(info["db"]["params"])
        elif info.get("request"):
            # WSGI call
            tags["http.path"] = info["request"]["path"]
            tags["http.query"] = info["request"]["query"]
            tags["http.method"] = info["request"]["method"]
            tags["http.scheme"] = info["request"]["scheme"]
        elif info.get("function"):
            # RPC, function calls
            if "args" in info["function"]:
                tags["args"] = info["function"]["args"]
            if "kwargs" in info["function"]:
                tags["kwargs"] = info["function"]["kwargs"]
            tags["name"] = info["function"]["name"]

        return tags
