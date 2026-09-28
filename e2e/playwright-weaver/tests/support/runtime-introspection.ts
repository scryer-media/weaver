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

/** The hardware profiles this instance offers and the one it has saved. */
export type HardwareProfileOffer = {
  selected: string | null;
  recommended: string;
  available: string[];
};

export async function introspectHardwareProfile(
  request: APIRequestContext,
): Promise<HardwareProfileOffer> {
  const data = await graphql<{ hardwareProfile: HardwareProfileOffer }>(
    request,
    `query WeaverE2EHardwareProfileOffer {
      hardwareProfile {
        selected
        recommended
        available
      }
    }`,
  );
  return data.hardwareProfile;
}
