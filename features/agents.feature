Feature: Spawning agents and seeing their output

  One agent primitive (DESIGN.md §5). A spawn never blocks: with nowhere to
  run it queues with its reason on record (§6), and a Worker joining later
  is what brings it to life. The Worker here is a real horde-worker
  supervising the scripted FakeAdapter — no provider login, real everything
  else.

  Scenario: A spawn with no worker queues with the reason on record
    Given a running server
    And a session titled "ci-fix"
    When the user runs "spawn <session>"
    Then the command succeeds
    And stdout contains "queued"
    And stdout contains "no worker is connected"

  Scenario: A joining worker runs the queued agent and its answer reaches the user
    Given a running server
    And a session titled "ci-fix"
    And a worker joins running a scripted agent that answers "the build is fixed now"
    When the user runs "send <root> \"fix the build\""
    Then the command succeeds
    And the user eventually sees "the build is fixed now" in the session replay

  Scenario: The usage display attributes what a turn consumed
    Given a running server
    And a session titled "ci-fix"
    And a worker joins running a scripted agent that answers "done"
    When the user runs "send <root> \"go\""
    Then the user eventually sees "done" in the session replay
    When the user runs "usage --by account"
    Then the command succeeds
    And stdout contains "usage by provider account (measured)"
    And stdout contains "in 1200"
    And stdout contains "agents 1"

  Scenario: An empty usage report says so instead of showing nothing
    Given a running server
    When the user runs "usage"
    Then the command succeeds
    And stdout contains "no usage recorded"
