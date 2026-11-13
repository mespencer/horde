Feature: Workspaces start fresh, check out what they need, and give disk back

  An agent's checkout starts from the latest commit on its Workspace's
  default branch, never from wherever the Instance was cloned, and the
  Instance's own tree is never moved (ADR-0014). A branch name may span
  every repo. Cleanup comes in two tiers: a soft reclaim runs a repo's
  reclaim command and keeps every commit; a hard removal deletes a checkout
  only when nothing can be lost, unless the operator forces it. Every
  cleanup has a dry run. The Worker here is a real horde-worker runtime
  with a workspace root, over real git repos.

  Background:
    Given a running server
    And an origin repo "atlas" with one commit
    And a workspace worker joins
    And a workspace "suite" over the origin repo "atlas"

  Scenario: A new agent starts from the origin's latest commit
    Given the origin repo "atlas" gains a commit after the instance was cloned
    When the user starts a session in workspace "suite"
    Then the user eventually sees the checkout start "fresh" at the origin's tip

  Scenario: One branch name is created in every repo the agent checks out
    When the user starts a session in workspace "suite" with "--branch feat/login"
    Then the user eventually sees the checkout start "fresh" on "feat/login"

  Scenario: A dry run says so and changes nothing
    When the user runs "workspace gc --hard --dry-run"
    Then the command succeeds
    And stdout contains "DRY RUN — nothing was changed"

  Scenario: Forcing a removal is only for a hard gc
    When the user runs "workspace gc --force"
    Then the command fails
