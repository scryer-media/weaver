import type { APIRequestContext } from "@playwright/test";

import { graphql } from "../helpers";

export async function introspectPublicMutationNames(
  request: APIRequestContext,
): Promise<string[]> {
  const data = await graphql<{
    __schema: { mutationType: { fields: Array<{ name: string }> } | null };
  }>(
    request,
    `query WeaverE2EMutationCoverage {
      __schema {
        mutationType {
          fields {
            name
          }
        }
      }
    }`,
  );

  return (data.__schema.mutationType?.fields ?? [])
    .map(({ name }) => name)
    .sort();
}
