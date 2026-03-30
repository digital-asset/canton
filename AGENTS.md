# Agent guidelines

## Purpose of this repo

This repo is a fork of the official canton network repo where we are adding the `external_call` extension. The changes we are making here in this repo must be upstream-friendly and optimized for acceptance by the upstream maintainers. That means the changes must be minimal, well-structured and defensible in review. The changes we are making here should remain scoped to the `external_call` extension and integrate seamlessly with existing code. We must reuse existing conventions and patterns established in the codebase and not introduce additional dependencies unless absolutely necessary.

## ExecPlans

When explicitly asked, use an ExecPlan (as described in .agent/PLANS.md) from design to implementation.