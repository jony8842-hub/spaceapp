import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { beforeAll, afterAll, describe, expect, it } from "vitest";
import { TestWorkflowEnvironment } from "@temporalio/testing";
import { historyToJSON } from "@temporalio/common/lib/proto-utils";
import { Worker, bundleWorkflowCode, type WorkflowBundle } from "@temporalio/worker";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const fixtureRoot = path.join(repoRoot, "apps/worker/tests/fixtures/workflow-histories");
const workflowEntry = path.join(repoRoot, "apps/worker/tests/support/temporal-legacy-workflows.ts");
const queue = "space-legacy-supervisor-fixture";

describe("legacy Temporal fixture recorder", () => {
  const enabled = process.env.TEMPORAL_RECORD_HISTORIES === "1";
  let environment: TestWorkflowEnvironment;
  let workflowBundle: WorkflowBundle;

  beforeAll(async () => {
    if (!enabled) return;
    workflowBundle = await bundleWorkflowCode({ workflowsPath: workflowEntry });
    environment = await TestWorkflowEnvironment.createTimeSkipping();
  }, 120_000);

  afterAll(async () => {
    await environment?.teardown();
  }, 120_000);

  it.skipIf(!enabled)("records a pre-patch supervisor waiting history", async () => {
    const worker = await Worker.create({
      connection: environment.nativeConnection,
      namespace: environment.namespace,
      taskQueue: queue,
      workflowBundle
    });
    const run = worker.run();
    const handle = await environment.client.workflow.start("roomAgentSupervisorWorkflow", {
      taskQueue: queue,
      workflowId: "fixture-room-agent-supervisor-prepatch",
      args: [{ roomId: "room-fixture", pending: [], monitoringMissionIds: [], interrupted: [], processedCount: 0 }]
    });
    await new Promise((resolve) => setTimeout(resolve, 250));
    await handle.terminate("fixture complete");
    const history = await handle.fetchHistory();
    await fs.mkdir(fixtureRoot, { recursive: true });
    await fs.writeFile(path.join(fixtureRoot, "room-agent-supervisor-prepatch.json"), `${historyToJSON(history)}\n`, "utf8");
    await worker.shutdown();
    await run;
    expect(history.events.length).toBeGreaterThan(1);
  }, 120_000);
});
