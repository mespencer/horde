Feature: Assignments, notes, and results

  Two message kinds, one bit of intent (DESIGN.md §5): an assignment owes
  exactly one result; a note owes nothing. Ancestry is fixed at spawn, so a
  tree's messages stay attributable. Results are observed facts — only the
  worker spine can author one, which is exactly how the step below writes it.

  Scenario: An assignment owes a result, and the result reaches the log
    Given a running server
    And a session titled "review"
    When the user runs "spawn <session> --parent <root>"
    Then the command succeeds
    And the spawned child's id is on stdout
    When the user runs "send <child> \"review the diff\""
    Then the command succeeds
    And stdout names the assignment it opened
    When the child reports success "looks good", journaled the way a worker journals it
    Then the result "looks good" is on the log
    And the session replay shows a reported result

  Scenario: A note owes no result
    Given a running server
    And a session titled "review"
    When the user runs "send <root> \"heads up: rebased\" --note"
    Then the command succeeds
    And stdout does not mention an assignment
    And the user eventually sees "heads up: rebased" in the session replay
