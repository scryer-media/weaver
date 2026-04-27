import { gql, type Client } from "urql";

type SharedVariableSpec = {
  type: string;
  value: unknown;
};

type ExecuteAliasedIdMutationOptions = {
  client: Client;
  ids: number[];
  operationName: string;
  aliasPrefix: string;
  fieldName: string;
  idType?: string;
  sharedVariables?: Record<string, SharedVariableSpec>;
  buildFieldArguments?: (idVariable: string) => string;
  selectionSet?: string;
  fragments?: string[];
};

export function buildAliasedIdMutation({
  ids,
  operationName,
  aliasPrefix,
  fieldName,
  idType = "Int!",
  sharedVariables = {},
  buildFieldArguments,
  selectionSet,
  fragments = [],
}: Omit<ExecuteAliasedIdMutationOptions, "client">) {
  const variableDefinitions = ids
    .map((_, index) => `$id${index}: ${idType}`)
    .concat(
      Object.entries(sharedVariables).map(
        ([name, spec]) => `$${name}: ${spec.type}`,
      ),
    )
    .join(", ");

  const fields = ids
    .map((_, index) => {
      const idVariable = `$id${index}`;
      const argumentsText = buildFieldArguments?.(idVariable) ?? `id: ${idVariable}`;
      const resultSelection = selectionSet ? ` ${selectionSet}` : "";
      return `  ${aliasPrefix}${index}: ${fieldName}(${argumentsText})${resultSelection}`;
    })
    .join("\n");

  const fragmentText = fragments.length > 0 ? `\n${fragments.join("\n")}` : "";
  return gql(`mutation ${operationName}(${variableDefinitions}) {\n${fields}\n}${fragmentText}`);
}

export function executeAliasedIdMutation<TData = boolean>({
  client,
  ids,
  sharedVariables = {},
  ...definition
}: ExecuteAliasedIdMutationOptions) {
  const mutation = buildAliasedIdMutation({
    ids,
    sharedVariables,
    ...definition,
  });
  const variables: Record<string, unknown> = {
    ...Object.fromEntries(ids.map((id, index) => [`id${index}`, id])),
    ...Object.fromEntries(
      Object.entries(sharedVariables).map(([name, spec]) => [name, spec.value]),
    ),
  };

  return client.mutation<Record<string, TData>, Record<string, unknown>>(mutation, variables).toPromise();
}