Feature: Enrollment and revocation

  Every principal speaks mTLS (DESIGN.md §3). A new machine joins with a single
  join token and nothing else — the keypair never travels. Revocation
  closes the door by serial, and the unenrolled never get past the
  handshake.

  Scenario: A new client machine joins with a one-time join token
    Given a secured server
    And an enrolled operator "laptop"
    When the operator runs "enroll client field-laptop"
    Then the command succeeds
    And stdout prints a one-time join token
    When a new machine joins with the printed token
    Then the new machine's identity opens the control plane

  Scenario: A minted worker code enrolls a worker, and revocation locks it out
    Given a secured server
    And an enrolled operator "laptop"
    When the operator runs "worker enroll gpu-box"
    Then the command succeeds
    And stdout prints a one-time join token
    When a worker machine joins as "gpu-box" with the printed token
    Then the worker is enrolled as "worker:gpu-box"
    When the operator runs "worker revoke gpu-box --reason compromised"
    Then the command succeeds
    And the worker's identity no longer opens the control plane

  Scenario: The unenrolled are refused at the door
    Given a secured server
    When an unenrolled user tries "hello" against the secured server
    Then the command fails
