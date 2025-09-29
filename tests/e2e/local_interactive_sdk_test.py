from codeflare_sdk import (
    Cluster,
    ClusterConfiguration,
    TokenAuthentication,
    generate_cert,
)

import pytest
import ray
import math
import subprocess
import time

from support import *


@pytest.mark.kind
@pytest.mark.openshift
class TestRayLocalInteractive:
    def setup_method(self):
        initialize_kubernetes_client(self)
        self.port_forward_process = None

    def cleanup_port_forward(self):
        if self.port_forward_process:
            self.port_forward_process.terminate()
            self.port_forward_process.wait(timeout=10)
            self.port_forward_process = None

    def teardown_method(self):
        self.cleanup_port_forward()
        delete_namespace(self)
        delete_kueue_resources(self)

    def test_local_interactives(self):
        self.setup_method()
        create_namespace(self)
        create_kueue_resources(self)
        self.run_local_interactives()

    @pytest.mark.nvidia_gpu
    @pytest.mark.skipif(
        is_openshift(), reason="GPU tests not supported on OpenShift test environment"
    )
    def test_local_interactives_nvidia_gpu(self):
        self.setup_method()
        create_namespace(self)
        create_kueue_resources(self)
        self.run_local_interactives(number_of_gpus=1)

    def run_local_interactives(
        self, gpu_resource_name="nvidia.com/gpu", number_of_gpus=0
    ):
        cluster_name = "test-ray-cluster-li"

        # Platform-specific authentication
        if is_openshift():
            auth = TokenAuthentication(
                token=run_oc_command(["whoami", "--show-token=true"]),
                server=run_oc_command(["whoami", "--show-server=true"]),
                skip_tls=True,
            )
            auth.login()

        ray.shutdown()

        # Platform-specific configuration
        config_args = {
            "name": cluster_name,
            "namespace": self.namespace,
            "num_workers": 1,
            "image": get_ray_image(),
            "verify_tls": False,
        }

        if is_openshift():
            # OpenShift uses different resource requirements
            config_args.update(
                {
                    "worker_cpu_requests": 1,
                    "worker_cpu_limits": 1,
                    "worker_memory_requests": 1,
                    "worker_memory_limits": 4,
                }
            )
        else:
            # Kind configuration
            config_args.update(
                {
                    "head_cpu_requests": "500m",
                    "head_cpu_limits": "500m",
                    "worker_cpu_requests": "500m",
                    "worker_cpu_limits": 1,
                    "worker_memory_requests": 1,
                    "worker_memory_limits": 4,
                    "worker_extended_resource_requests": {
                        gpu_resource_name: number_of_gpus
                    },
                }
            )

        cluster = Cluster(ClusterConfiguration(**config_args))
        cluster.up()
        cluster.wait_ready()
        cluster.status()

        generate_cert.generate_tls_cert(cluster_name, self.namespace)
        generate_cert.export_env(cluster_name, self.namespace)

        # Define Ray tasks
        @ray.remote(num_gpus=number_of_gpus / 2 if number_of_gpus > 0 else 0)
        def heavy_calculation_part(num_iterations):
            result = 0.0
            for i in range(num_iterations):
                for j in range(num_iterations):
                    for k in range(num_iterations):
                        result += math.sin(i) * math.cos(j) * math.tan(k)
            return result

        @ray.remote(num_gpus=number_of_gpus / 2 if number_of_gpus > 0 else 0)
        def heavy_calculation(num_iterations):
            results = ray.get(
                [heavy_calculation_part.remote(num_iterations // 30) for _ in range(30)]
            )
            return sum(results)

        # Platform-specific connection method
        try:
            if is_openshift():
                # OpenShift: Direct connection
                ray.init(address=cluster.local_client_url(), logging_level="DEBUG")
            else:
                # Kind: Port forwarding required
                local_port = "20001"
                ray_client_port = "10001"

                port_forward_cmd = [
                    "kubectl",
                    "port-forward",
                    "-n",
                    self.namespace,
                    f"svc/{cluster_name}-head-svc",
                    f"{local_port}:{ray_client_port}",
                ]
                self.port_forward_process = subprocess.Popen(
                    port_forward_cmd,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )

                # Give port forward time to establish
                time.sleep(3)

                client_url = f"ray://localhost:{local_port}"
                ray.init(address=client_url, logging_level="INFO")

            # Run the computation
            ref = heavy_calculation.remote(3000)
            result = ray.get(ref)
            assert result == 1789.4644387076714
            ray.cancel(ref)
            ray.shutdown()

            cluster.down()
        finally:
            self.cleanup_port_forward()
