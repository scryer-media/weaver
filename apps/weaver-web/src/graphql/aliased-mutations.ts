// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { gql, type Client, type CombinedError } from "urql";

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

/**
 * The most aliases one request carries. Each counts against the daemon's
 * query complexity limit, so a selection of any size goes in groups well
 * under it.
 */
export const ALIASED_GROUP_SIZE = 200;

/**
 * Run the mutation over `ids` in groups of at most `ALIASED_GROUP_SIZE`, one
 * group at a time. The data is keyed by each id's place in `ids`, as one
 * request over them all would key it. A group that fails leaves its ids out
 * of the data and the rest still run; the first failure is returned.
 */
export async function executeAliasedIdMutation<TData = boolean>({
  client,
  ids,
  sharedVariables = {},
  ...definition
}: ExecuteAliasedIdMutationOptions): Promise<{ data: Record<string, TData>; error?: CombinedError }> {
  const data: Record<string, TData> = {};
  let error: CombinedError | undefined;
  for (let start = 0; start < ids.length; start += ALIASED_GROUP_SIZE) {
    const group = ids.slice(start, start + ALIASED_GROUP_SIZE);
    const mutation = buildAliasedIdMutation({ ids: group, sharedVariables, ...definition });
    const variables: Record<string, unknown> = {
      ...Object.fromEntries(group.map((id, index) => [`id${index}`, id])),
      ...Object.fromEntries(Object.entries(sharedVariables).map(([name, spec]) => [name, spec.value])),
    };
    const result = await client
      .mutation<Record<string, TData>, Record<string, unknown>>(mutation, variables)
      .toPromise();
    error ??= result.error;
    group.forEach((_id, index) => {
      const value = result.data?.[`${definition.aliasPrefix}${index}`];
      if (value !== undefined) {
        data[`${definition.aliasPrefix}${start + index}`] = value;
      }
    });
  }
  return { data, error };
}
