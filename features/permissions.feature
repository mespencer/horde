Feature: Permission escalations

  The permission chain ends at a human (DESIGN.md §10): an escalated tool
  call rides the event log like everything else, answerable from any
  connected Client, and the decision — allow, deny, or allow with narrowed
  arguments — travels back down to the agent that asked, which is blocked
  until it does.

  Scenario: Approving lets the waiting agent proceed
    Given a running server
    And a session titled "ci-fix"
    And a worker joins running a scripted agent that must ask before running "docker rm cache", then answers "removed it"
    When the user runs "send <root> \"clean up\""
    And the escalation to run "docker rm cache" reaches the log
    And the user runs "approve <request>"
    Then the command succeeds
    And the decision on the log is "allowed"
    And the user eventually sees "removed it" in the session replay

  Scenario: Denying carries the reason back
    Given a running server
    And a session titled "ci-fix"
    And a worker joins running a scripted agent that must ask before running "docker rm cache", then answers "understood"
    When the user runs "send <root> \"clean up\""
    And the escalation to run "docker rm cache" reaches the log
    And the user runs "approve <request> --deny --reason \"not on this box\""
    Then the command succeeds
    And the decision on the log is "denied"
    And the decision's reason is "not on this box"
    And the user eventually sees "understood" in the session replay

  Scenario: Narrowing clamps the call's arguments instead of merely permitting it
    Given a running server
    And a session titled "ci-fix"
    And a worker joins running a scripted agent that must ask before running "docker rm cache", then answers "dry run only"
    When the user runs "send <root> \"clean up\""
    And the escalation to run "docker rm cache" reaches the log
    And the user approves "<request>" narrowing the command to "docker rm cache --dry-run"
    Then the command succeeds
    And the decision on the log carries the narrowed command "docker rm cache --dry-run"
    And the user eventually sees "dry run only" in the session replay
