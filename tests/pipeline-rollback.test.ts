/**
 * Tests for pipeline rollback (reject/retry) support.
 *
 * When a step outputs STATUS: retry or STATUS: reject and has `reject_to`
 * (from on_fail.retry_step), the pipeline should roll back to the target step
 * instead of advancing forward.
 */

import { describe, it, afterEach } from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import { getDb } from "../dist/db.js";
import { completeStep } from "../dist/installer/step-ops.js";

// ── Helpers ──────────────────────────────────────────────────────────

function now(): string {
  return new Date().toISOString();
}

function createRun(opts: { runId: string; workflowId?: string; context?: Record<string, string> }) {
  const db = getDb();
  db.prepare(
    "INSERT INTO runs (id, workflow_id, task, status, context, created_at, updated_at) VALUES (?, ?, 'test task', 'running', ?, ?, ?)"
  ).run(
    opts.runId,
    opts.workflowId ?? "test-wf",
    JSON.stringify(opts.context ?? {}),
    now(),
    now()
  );
}

function createStep(opts: {
  id?: string;
  runId: string;
  stepId: string;
  agentId?: string;
  stepIndex: number;
  status: string;
  type?: string;
  loopConfig?: string | null;
  rejectTo?: string | null;
  maxRetries?: number;
  retryCount?: number;
}): string {
  const db = getDb();
  const dbId = opts.id ?? crypto.randomUUID();
  db.prepare(
    `INSERT INTO steps
      (id, run_id, step_id, agent_id, step_index, input_template, expects, status, type, loop_config, reject_to, max_retries, retry_count, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?, '', 'STATUS: done', ?, ?, ?, ?, ?, ?, ?, ?)`
  ).run(
    dbId,
    opts.runId,
    opts.stepId,
    opts.agentId ?? "test-agent",
    opts.stepIndex,
    opts.status,
    opts.type ?? "single",
    opts.loopConfig ?? null,
    opts.rejectTo ?? null,
    opts.maxRetries ?? 2,
    opts.retryCount ?? 0,
    now(),
    now()
  );
  return dbId;
}

function createStory(opts: {
  runId: string;
  storyId?: string;
  status?: string;
  storyIndex?: number;
}): string {
  const db = getDb();
  const id = crypto.randomUUID();
  db.prepare(
    `INSERT INTO stories
      (id, run_id, story_index, story_id, title, description, acceptance_criteria, status, retry_count, max_retries, created_at, updated_at)
     VALUES (?, ?, ?, ?, 'Test Story', 'desc', '[]', ?, 0, 2, ?, ?)`
  ).run(
    id,
    opts.runId,
    opts.storyIndex ?? 0,
    opts.storyId ?? "S1",
    opts.status ?? "done",
    now(),
    now()
  );
  return id;
}

function cleanup(runId: string) {
  const db = getDb();
  db.prepare("DELETE FROM stories WHERE run_id = ?").run(runId);
  db.prepare("DELETE FROM steps WHERE run_id = ?").run(runId);
  db.prepare("DELETE FROM runs WHERE id = ?").run(runId);
}

// ── Tests ─────────────────────────────────────────────────────────────

describe("pipeline rollback (reject/retry)", () => {
  const runIds: string[] = [];
  afterEach(() => {
    for (const id of runIds) cleanup(id);
    runIds.length = 0;
  });

  it("rolls back to target step when STATUS: retry and reject_to is set", async () => {
    const runId = crypto.randomUUID();
    runIds.push(runId);
    createRun({ runId });

    // fix step (index 0, target)
    const fixId = createStep({ runId, stepId: "fix", stepIndex: 0, status: "done" });
    // verify step (index 1, current) — has reject_to: fix
    const verifyId = createStep({
      runId,
      stepId: "verify",
      stepIndex: 1,
      status: "running",
      rejectTo: "fix",
      maxRetries: 2,
    });

    const result = await completeStep(verifyId, "STATUS: retry\nISSUES:\n- Test failed");

    assert.ok(result.advanced, "should report advanced (rolled back)");

    const db = getDb();
    const fixStep = db.prepare("SELECT status FROM steps WHERE id = ?").get(fixId) as { status: string };
    const verifyStep = db.prepare("SELECT status FROM steps WHERE id = ?").get(verifyId) as { status: string };
    const run = db.prepare("SELECT status FROM runs WHERE id = ?").get(runId) as { status: string };

    assert.equal(fixStep.status, "pending", "fix step should be reset to pending");
    assert.equal(verifyStep.status, "waiting", "verify step should be waiting for re-run");
    assert.equal(run.status, "running", "run should still be running");
  });

  it("stores reject feedback in run context", async () => {
    const runId = crypto.randomUUID();
    runIds.push(runId);
    createRun({ runId });

    createStep({ runId, stepId: "fix", stepIndex: 0, status: "done" });
    const verifyId = createStep({
      runId,
      stepId: "verify",
      stepIndex: 1,
      status: "running",
      rejectTo: "fix",
    });

    await completeStep(verifyId, "STATUS: retry\nISSUES:\n- Regression found in edge case");

    const db = getDb();
    const run = db.prepare("SELECT context FROM runs WHERE id = ?").get(runId) as { context: string };
    const ctx = JSON.parse(run.context) as Record<string, string>;
    assert.ok(ctx["reject_feedback"], "reject_feedback should be stored in context");
    assert.ok(ctx["reject_feedback"].includes("Regression found in edge case"), "feedback content preserved");
  });

  it("increments retry_count on the rejecting step", async () => {
    const runId = crypto.randomUUID();
    runIds.push(runId);
    createRun({ runId });

    createStep({ runId, stepId: "fix", stepIndex: 0, status: "done" });
    const verifyId = createStep({
      runId,
      stepId: "verify",
      stepIndex: 1,
      status: "running",
      rejectTo: "fix",
    });

    await completeStep(verifyId, "STATUS: retry\nISSUES:\n- Issue A");

    const db = getDb();
    const step = db.prepare("SELECT retry_count FROM steps WHERE id = ?").get(verifyId) as { retry_count: number };
    assert.equal(step.retry_count, 1, "retry_count should be incremented to 1");
  });

  it("fails run when retry_count exceeds max_retries", async () => {
    const runId = crypto.randomUUID();
    runIds.push(runId);
    createRun({ runId });

    createStep({ runId, stepId: "fix", stepIndex: 0, status: "done" });
    const verifyId = createStep({
      runId,
      stepId: "verify",
      stepIndex: 1,
      status: "running",
      rejectTo: "fix",
      maxRetries: 1,
      retryCount: 1, // already at max
    });

    const result = await completeStep(verifyId, "STATUS: retry\nISSUES:\n- Still broken");

    assert.ok(!result.runCompleted, "run should not complete");

    const db = getDb();
    const run = db.prepare("SELECT status FROM runs WHERE id = ?").get(runId) as { status: string };
    assert.equal(run.status, "failed", "run should fail when retries exhausted");

    const step = db.prepare("SELECT status FROM steps WHERE id = ?").get(verifyId) as { status: string };
    assert.equal(step.status, "failed", "verify step should be failed");
  });

  it("also handles STATUS: reject as a rollback signal", async () => {
    const runId = crypto.randomUUID();
    runIds.push(runId);
    createRun({ runId });

    createStep({ runId, stepId: "implement", stepIndex: 0, status: "done", type: "loop" });
    const testId = createStep({
      runId,
      stepId: "test",
      stepIndex: 1,
      status: "running",
      rejectTo: "implement",
    });

    const result = await completeStep(testId, "STATUS: reject\nFAILURES:\n- Integration test failed");

    assert.ok(result.advanced, "should report advanced");

    const db = getDb();
    const testStep = db.prepare("SELECT status FROM steps WHERE id = ?").get(testId) as { status: string };
    assert.equal(testStep.status, "waiting", "test step should be waiting");
  });

  it("resets all non-failed stories to pending when rolling back to a loop step", async () => {
    const runId = crypto.randomUUID();
    runIds.push(runId);
    createRun({ runId });

    const implementId = createStep({
      runId,
      stepId: "implement",
      stepIndex: 0,
      status: "done",
      type: "loop",
      loopConfig: JSON.stringify({ over: "stories", completion: "all_done" }),
    });
    const testId = createStep({
      runId,
      stepId: "test",
      stepIndex: 1,
      status: "running",
      rejectTo: "implement",
    });

    // Create two done stories
    const story1Id = createStory({ runId, storyId: "S1", status: "done", storyIndex: 0 });
    const story2Id = createStory({ runId, storyId: "S2", status: "done", storyIndex: 1 });

    await completeStep(testId, "STATUS: retry\nFAILURES:\n- E2E test failed");

    const db = getDb();
    const s1 = db.prepare("SELECT status FROM stories WHERE id = ?").get(story1Id) as { status: string };
    const s2 = db.prepare("SELECT status FROM stories WHERE id = ?").get(story2Id) as { status: string };
    const impl = db.prepare("SELECT status FROM steps WHERE id = ?").get(implementId) as { status: string };

    assert.equal(s1.status, "pending", "story 1 should be reset to pending");
    assert.equal(s2.status, "pending", "story 2 should be reset to pending");
    assert.equal(impl.status, "pending", "loop step should be reset to pending");
  });

  it("does NOT roll back when reject_to is not set (no on_fail.retry_step)", async () => {
    const runId = crypto.randomUUID();
    runIds.push(runId);
    createRun({ runId });

    const fixId = createStep({ runId, stepId: "fix", stepIndex: 0, status: "done" });
    const verifyId = createStep({
      runId,
      stepId: "verify",
      stepIndex: 1,
      status: "running",
      rejectTo: null, // no reject_to
    });

    await completeStep(verifyId, "STATUS: retry\nISSUES:\n- Something wrong");

    const db = getDb();
    const fixStep = db.prepare("SELECT status FROM steps WHERE id = ?").get(fixId) as { status: string };
    const verifyStep = db.prepare("SELECT status FROM steps WHERE id = ?").get(verifyId) as { status: string };

    // Without reject_to, verify step should be marked done and fix step unchanged
    assert.equal(verifyStep.status, "done", "verify step marked done (no rollback)");
    assert.equal(fixStep.status, "done", "fix step unchanged");
  });

  it("resets intermediate steps to waiting during rollback", async () => {
    const runId = crypto.randomUUID();
    runIds.push(runId);
    createRun({ runId });

    const planId = createStep({ runId, stepId: "plan", stepIndex: 0, status: "done" });
    const implementId = createStep({ runId, stepId: "implement", stepIndex: 1, status: "done" });
    const testId = createStep({ runId, stepId: "test", stepIndex: 2, status: "done" });
    // review rolls back to plan
    const reviewId = createStep({
      runId,
      stepId: "review",
      stepIndex: 3,
      status: "running",
      rejectTo: "plan",
    });

    await completeStep(reviewId, "STATUS: retry\nFEEDBACK:\n- Rethink the approach");

    const db = getDb();
    const plan = db.prepare("SELECT status FROM steps WHERE id = ?").get(planId) as { status: string };
    const impl = db.prepare("SELECT status FROM steps WHERE id = ?").get(implementId) as { status: string };
    const test = db.prepare("SELECT status FROM steps WHERE id = ?").get(testId) as { status: string };
    const review = db.prepare("SELECT status FROM steps WHERE id = ?").get(reviewId) as { status: string };

    assert.equal(plan.status, "pending", "plan (target) should be pending");
    assert.equal(impl.status, "waiting", "implement (intermediate) should be waiting");
    assert.equal(test.status, "waiting", "test (intermediate) should be waiting");
    assert.equal(review.status, "waiting", "review (current) should be waiting");
  });

  it("does not rollback when STATUS is done", async () => {
    const runId = crypto.randomUUID();
    runIds.push(runId);
    createRun({ runId });

    createStep({ runId, stepId: "fix", stepIndex: 0, status: "done" });
    const verifyId = createStep({
      runId,
      stepId: "verify",
      stepIndex: 1,
      status: "running",
      rejectTo: "fix",
    });

    await completeStep(verifyId, "STATUS: done\nVERIFIED: All tests pass");

    const db = getDb();
    const step = db.prepare("SELECT status FROM steps WHERE id = ?").get(verifyId) as { status: string };
    assert.equal(step.status, "done", "step should be marked done when STATUS: done");
  });
});
