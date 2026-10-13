export type DuplicateSubmissionMode = "ENFORCE" | "SCORE" | "ALL" | "FORCE";

export type DuplicateSubmissionVariables = {
  force: boolean;
  dupeMode: DuplicateSubmissionMode;
  dupeKey: string | null;
  dupeScore: number | null;
};

export function duplicateSubmissionInput(input: {
  force: boolean;
  mode: DuplicateSubmissionMode;
  key: string;
  score: string;
}): DuplicateSubmissionVariables {
  const mode = input.force ? "FORCE" : input.mode;
  const parsedScore = Number(input.score);
  const score = Number.isSafeInteger(parsedScore) ? parsedScore : null;

  return {
    force: mode === "FORCE",
    dupeMode: mode,
    dupeKey: mode === "SCORE" ? input.key.trim() || null : null,
    dupeScore: mode === "SCORE" ? score : null,
  };
}
