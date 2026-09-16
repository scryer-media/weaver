import { gql } from "urql";

/**
 * Queries this UI asks for in its own shape.
 *
 * Everything else comes from `@/graphql/queries`, which both interfaces share.
 * These exist because the Next screens read fields the classic table never
 * asked for — history rows carry how long a job took, and a failed one says
 * why — and widening the shared fragment would put those fields on the classic
 * page's wire for nothing. No new server capability is involved: every field
 * below is one `HistoryItem` already exposes.
 */

export const NEXT_HISTORY_PAGE_QUERY = gql`
  query NextHistoryPage($input: HistoryPageInput!) {
    historyPage(input: $input) {
      items {
        id
        name
        displayTitle
        originalTitle
        status: state
        error
        totalBytes
        downloadedBytes
        health
        category
        createdAt
        completedAt
        deleteOperation {
          operationId
          state
          locked
          deleteFiles
          errorMessage
        }
      }
      totalCount
      counts {
        all
        success
        failure
      }
    }
  }
`;
