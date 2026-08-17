Feature: Sessions and attachment

  A Session is one conversation tree rooted at one top-level Agent
  (DESIGN.md §6, §11). Creating one is the first thing a user ever does,
  and the ids it puts on screen are what every later verb needs. Attaching
  is joining the session's live view; detaching returns to the overview.

  Scenario: Creating a session puts the root agent's id on screen
    Given a running server
    When the user runs "session new ci-fix"
    Then the command succeeds
    And stdout names the root agent
    When the user runs "session list"
    Then the command succeeds
    And stdout contains "ci-fix"

  Scenario: An empty listing says so instead of showing nothing
    Given a running server
    When the user runs "session list"
    Then the command succeeds
    And stdout says "no sessions"

  Scenario: Archiving hides a session from the listing without deleting it
    Given a running server
    And a session titled "old-work"
    When the user runs "session archive <session> --reason done"
    Then the command succeeds
    When the user runs "session list"
    Then stdout says "no sessions"
    When the user runs "session list --include-archived"
    Then stdout contains "old-work"

  Scenario: Attaching shows what the session's agent already said
    Given a running server
    And a session titled "ci-fix"
    And the agent has already said "the build is fixed"
    When the user attaches to the session
    Then the attached view shows "the build is fixed"
    When the user detaches
    Then the overview lists "ci-fix"
