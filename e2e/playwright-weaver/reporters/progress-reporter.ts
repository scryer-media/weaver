import fs from "node:fs";
import path from "node:path";
import type {
  FullConfig,
  Reporter,
  Suite,
  TestCase,
  TestResult,
  TestStep,
} from "@playwright/test/reporter";

type ProgressSnapshot = {
  totalTests: number;
  completedTests: number;
  currentTestTitle: string;
  currentStepTitle: string;
  stepsCompletedInTest: number;
  updatedAt: string;
};

export default class ProgressReporter implements Reporter {
  private readonly outputDir: string;
  private totalTests = 0;
  private completedTests = 0;
  private currentTestTitle = "";
  private currentStepTitle = "";
  private stepsCompletedInTest = 0;

  constructor(options: { outputDir?: string } = {}) {
    this.outputDir = options.outputDir || "artifacts";
  }

  onBegin(_config: FullConfig, suite: Suite) {
    this.totalTests = suite.allTests().length;
    this.write();
  }

  onTestBegin(test: TestCase) {
    this.currentTestTitle = test.titlePath().join(" › ");
    this.currentStepTitle = "";
    this.stepsCompletedInTest = 0;
    this.write();
  }

  onStepBegin(_test: TestCase, _result: TestResult, step: TestStep) {
    if (step.category === "test.step") {
      this.currentStepTitle = step.title;
      this.write();
    }
  }

  onStepEnd(_test: TestCase, _result: TestResult, step: TestStep) {
    if (step.category === "test.step") {
      this.stepsCompletedInTest += 1;
      this.write();
    }
  }

  onTestEnd() {
    this.completedTests += 1;
    this.write();
  }

  private write() {
    fs.mkdirSync(this.outputDir, { recursive: true });
    const snapshot: ProgressSnapshot = {
      totalTests: this.totalTests,
      completedTests: this.completedTests,
      currentTestTitle: this.currentTestTitle,
      currentStepTitle: this.currentStepTitle,
      stepsCompletedInTest: this.stepsCompletedInTest,
      updatedAt: new Date().toISOString(),
    };
    const target = path.join(this.outputDir, "progress.json");
    const temporary = `${target}.tmp-${process.pid}`;
    fs.writeFileSync(temporary, JSON.stringify(snapshot));
    fs.renameSync(temporary, target);
  }
}
