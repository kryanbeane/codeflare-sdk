# Kueue 2-Team Demo: Queueing and Preemption

This demo showcases the core Kueue concepts:
- **ClusterQueues** - Resource quotas per team
- **LocalQueues** - Team access points to ClusterQueues
- **Queueing** - Jobs wait when quota is full
- **Preemption** - Higher-priority jobs evict lower-priority jobs

## Demo Scenario: "Queueing and Priority-Based Preemption"

**Goal:** Demonstrate how Kueue manages job queueing when quotas are full and how higher-priority jobs can preempt lower-priority jobs.

### Setup

- **Two Teams with ClusterQueues in a Cohort:**
  - `team-research`: Has `cq-research` with quota for 2 workloads (1.5 CPU, 20Gi)
  - `team-prod`: Has `cq-prod` with quota for 1 workload (0.75 CPU, 10Gi)
  - **Both in "main-cohort"** - enables borrowing and cross-CQ preemption

- **Priority Classes:**
  - `low-priority` (10): Regular research workloads
  - `default-priority` (100): Production workloads
  - `high-priority` (1000): Critical/urgent workloads

### What Will Happen

1. **research-team-cluster** (default-priority) - Uses cq-research quota (0.75 CPU, 10Gi)
2. **prod-team-cluster** (default-priority) - Uses cq-prod quota (0.75 CPU, 10Gi, cq-prod full)
3. **research-low-priority-job** (low-priority) - Uses remaining cq-research quota (0.75 CPU, 10Gi, cq-research full)
4. **prod-critical-job** (high-priority) submitted to cq-prod:
   - cq-prod quota is full, so it must borrow from cohort
   - **PREEMPTS** the low-priority research job (cross-CQ preemption!)
   - Reclaims that borrowed capacity for the high-priority prod job
   - Long-lived clusters continue running (protected by maxPriorityThreshold)

## Prerequisites

- Kubernetes or OpenShift cluster
- Kueue operator installed (CRDs must be available)
- KubeRay operator installed

## Running the Demo

### Step 1: Apply Kueue Configuration

Apply the Kueue configuration files in order:

```bash
# 1. Create priority classes (defines job priorities)
kubectl apply -f kueue-priorities.yaml

# 2. Create resource flavors (defines resource types)
kubectl apply -f kueue-flavors.yaml

# 3. Create cluster queues (defines per-team resource quotas)
kubectl apply -f kueue-clusterqueues.yaml

# 4. Create team namespaces and local queues (team setup)
kubectl apply -f kueue-team-setup.yaml
```

### Step 2: Verify Kueue Setup

```bash
# Check that priority classes were created
kubectl get workloadpriorityclass

# Check that resource flavors were created
kubectl get resourceflavor

# Check that cluster queues were created
kubectl get clusterqueue

# Check that local queues were created in each namespace
kubectl get localqueue -n team-research
kubectl get localqueue -n team-prod
```

### Step 3: Create Long-Lived Clusters (Pre-Demo Setup)

Before the demo, create two long-lived clusters with default priority:

```python
from codeflare_sdk import Cluster, ClusterConfiguration

# Research team long-lived cluster
research_cluster = Cluster(ClusterConfiguration(
    name='research-team-cluster',
    namespace='team-research',
    head_cpu_requests='500m',
    head_cpu_limits='500m',
    head_memory_requests=6,
    head_memory_limits=8,
    num_workers=1,
    worker_cpu_requests='250m',
    worker_cpu_limits=1,
    worker_memory_requests=4,
    worker_memory_limits=6,
    local_queue="lq-research"
))
research_cluster.up()

# Prod team long-lived cluster
prod_cluster = Cluster(ClusterConfiguration(
    name='prod-team-cluster',
    namespace='team-prod',
    head_cpu_requests='500m',
    head_cpu_limits='500m',
    head_memory_requests=6,
    head_memory_limits=8,
    num_workers=1,
    worker_cpu_requests='250m',
    worker_cpu_limits=1,
    worker_memory_requests=4,
    worker_memory_limits=6,
    local_queue="lq-prod"
))
prod_cluster.up()
```

Verify both clusters are running:
```bash
kubectl get raycluster -A
kubectl get workloads -A
```

### Step 4: Submit Low-Priority Job

Now submit a low-priority job to fill the remaining quota:

```bash
# Submit low-priority research job
kubectl apply -f workloads-research.yaml
```

Observe the state:
```bash
# Watch workloads - should see 3 running (2 default + 1 low)
kubectl get workloads -n team-research

# Check cluster queue status - quota should be full
kubectl describe clusterqueue cq-research
```

### Step 5: Submit High-Priority Job (Triggers Preemption)

```bash
# Submit the high-priority critical job
kubectl apply -f workload-high-priority.yaml
```

### Step 6: Observe Cross-ClusterQueue Preemption

```bash
# Watch workloads in BOTH namespaces to see cross-CQ preemption
kubectl get workloads -A -w

# Check events to see preemption
kubectl get events -n team-research --sort-by='.lastTimestamp'
kubectl get events -n team-prod --sort-by='.lastTimestamp'

# Expected behavior:
# - Long-lived clusters (default priority) continue running in both CQs
# - High-priority prod job borrows from cohort
# - Low-priority research job is PREEMPTED (cross-CQ preemption!)
# - Low-priority job is evicted and goes to Queued state
# - High-priority prod job starts Running
```

## Key Observations

1. **Per-Team Quotas:** Each CQ has different quotas (research: 2 workloads, prod: 1 workload)
2. **Cohort Borrowing:** cq-prod can borrow idle capacity from cq-research via the cohort
3. **Cross-CQ Preemption:** High-priority job in cq-prod preempts low-priority job in cq-research
4. **Priority Protection:** `maxPriorityThreshold: 50` ensures default-priority long-lived clusters (priority 100) are NEVER preempted
5. **Resource Efficiency:** Demonstrates dynamic CPU/memory allocation across teams with priority-based fairness

## GPU Support (Optional)

This demo is configured for CPU-only workloads. To enable GPU support:
1. Ensure GPU nodes are available with `nvidia.com/gpu` resources
2. Uncomment the GPU resourceGroups in `kueue-clusterqueues.yaml`
3. Add GPU requests to your workload specs

The same cohort borrowing and preemption principles apply to GPU resources!

## Inspecting Workload Details

```bash
# Describe a workload to see detailed status and admission info
kubectl describe workload -n team-research

# Check cluster queue status
kubectl describe clusterqueue cq-research
kubectl describe clusterqueue cq-prod
```

## Testing High-Priority Preemption (Optional)

To test the complete 3-tier preemption hierarchy, you can create a high-priority job that will preempt the prod job:

```bash
# Create a critical job with high-priority
kubectl apply -f - <<EOF
apiVersion: ray.io/v1
kind: RayJob
metadata:
  name: critical-job-urgent
  namespace: team-prod
  labels:
    kueue.x-k8s.io/queue-name: "lq-prod"
    kueue.x-k8s.io/priority-class: "high-priority"
spec:
  entrypoint: python -c "import ray, time; ray.init(); print('CRITICAL job started - highest priority'); time.sleep(30); print('CRITICAL job finished'); ray.shutdown()"
  shutdownAfterJobFinishes: true
  ttlSecondsAfterFinished: 600
  rayClusterSpec:
    rayVersion: '2.47.1'
    headGroupSpec:
      rayStartParams:
        dashboard-host: '0.0.0.0'
        block: 'true'
      template:
        spec:
          containers:
          - name: ray-head
            image: quay.io/project-codeflare/ray:latest-py39-cu118
            resources:
              requests:
                cpu: "1"
                memory: "2Gi"
              limits:
                cpu: "1"
                memory: "2Gi"
    workerGroupSpecs:
    - groupName: worker-group
      replicas: 1
      minReplicas: 1
      maxReplicas: 1
      rayStartParams:
        block: 'true'
      template:
        spec:
          containers:
          - name: ray-worker
            image: quay.io/project-codeflare/ray:latest-py39-cu118
            resources:
              requests:
                cpu: "1"
                memory: "2Gi"
              limits:
                cpu: "1"
                memory: "2Gi"
EOF

# This high-priority job can preempt the default-priority prod job
kubectl get workloads -A -w
```

## Cleanup

```bash
# Delete all workloads
kubectl delete -f workload-high-priority.yaml
kubectl delete -f workloads-research.yaml

# Delete team setup (this also deletes the namespaces)
kubectl delete -f kueue-team-setup.yaml

# Delete Kueue configuration
kubectl delete -f kueue-clusterqueues.yaml
kubectl delete -f kueue-flavors.yaml
kubectl delete -f kueue-priorities.yaml
```

## Files in This Demo

1. **kueue-priorities.yaml** - WorkloadPriorityClass definitions (low, default, high)
2. **kueue-flavors.yaml** - ResourceFlavor definitions (cpu-flavor, gpu-flavor)
3. **kueue-clusterqueues.yaml** - ClusterQueue configurations (independent, no cohort)
4. **kueue-team-setup.yaml** - Namespaces and LocalQueues for each team
5. **workloads-research.yaml** - Three low-priority RayJobs demonstrating queueing
6. **workload-high-priority.yaml** - One high-priority RayJob demonstrating preemption

## Understanding the Configuration

### Priority Classes

The demo includes three priority levels demonstrating a complete preemption hierarchy:

- `low-priority` (value: 10): Used by research jobs, can be preempted by all higher priorities
- `default-priority` (value: 100): Used by prod jobs, can preempt low-priority, preempted by high-priority
- `high-priority` (value: 1000): For critical/urgent jobs, can preempt all lower priorities

### ClusterQueue Configuration

Both ClusterQueues are in the **main-cohort** to enable borrowing and cross-CQ preemption:

**Resource Quotas (CPU-only):**
- **cq-research**: 2.5 CPU, 25Gi memory (supports 2 workloads + submitters)
- **cq-prod**: 1.5 CPU, 15Gi memory (supports 1 workload + submitters)
- **Cohort total**: 4 CPU, 40Gi (supports 3 workloads + submitter overhead)

**Preemption & Borrowing:**
- **Preemption**: `reclaimWithinCohort: Any` allows higher-priority jobs to reclaim borrowed resources
- **Borrowing**: `borrowWithinCohort.policy: LowerPriority` allows low-priority jobs to use idle cohort capacity
- **Protection**: `maxPriorityThreshold: 50` ensures borrowing ONLY preempts low-priority workloads (≤50), protecting default-priority (100) long-lived clusters

### Workload Specifications

**All Workloads (CPU-only):**
- **Head node**: 500m CPU, 6Gi memory
- **Worker node (1 replica)**: 250m CPU, 4Gi memory
- **Total per workload**: 0.75 CPU, 10Gi memory

This configuration demonstrates:
- Each ClusterQueue has quota for specific number of workloads
- Jobs can borrow idle capacity from other CQs in the cohort
- cq-prod has minimal quota (1 workload), so additional jobs must borrow
- High-priority job in cq-prod will borrow and preempt low-priority job in cq-research
- Default-priority long-lived clusters are protected from preemption by `maxPriorityThreshold`

## Troubleshooting

### Workload stays in "Pending" state

```bash
kubectl describe workload <workload-name> -n <namespace>
```

Check the "Conditions" section for why it's pending (e.g., insufficient quota).

### Job doesn't get preempted

- Verify priority classes are correctly assigned
- Check that `withinClusterQueue: LowerPriority` is set on the ClusterQueue
- Ensure jobs are in the same ClusterQueue

### RayJob fails to start

```bash
kubectl describe rayjob <job-name> -n <namespace>
kubectl logs -n <namespace> <pod-name>
```

Check for Ray-specific errors (image pull, resource constraints, etc.).

## Additional Resources

- [Kueue Documentation](https://kueue.sigs.k8s.io/)
- [KubeRay Documentation](https://docs.ray.io/en/latest/cluster/kubernetes/index.html)
- [CodeFlare SDK Documentation](https://github.com/project-codeflare/codeflare-sdk)
