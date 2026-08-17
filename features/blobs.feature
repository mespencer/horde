Feature: Attachments through the blob store

  Non-code payloads move as content-addressed blobs (DESIGN.md §8): stash
  prints the address, fetch verifies what it hands back against that
  address before keeping anything, and content nobody stashed is a loud
  failure rather than an empty file.

  Scenario: Stash then fetch round trips the bytes, verified
    Given a running server with a blob store
    And a work file "build.log" containing "error[E0308]: forty screens of it"
    When the user stashes "build.log"
    Then the command succeeds
    And stdout prints the blob's address
    When the user fetches that address into "fetched.log"
    Then the command succeeds
    And stdout contains "verified"
    And the fetched file "fetched.log" holds "error[E0308]: forty screens of it"

  Scenario: Fetching an address nobody stashed fails loudly
    Given a running server with a blob store
    When the user fetches the address of the missing content "never stashed" into "missing.log"
    Then the command fails
    And no output file was written
