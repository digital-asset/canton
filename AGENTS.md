# Agent Guidelines

## Purpose Of This Repo

This repo is a fork of the official canton network repo where we are adding the `external_call` extension.

The changes we make here must:

- Be upstream-friendly and optimized for acceptance by the upstream maintainers.
- Be minimal, well-structured, and defensible in review.
- Remain scoped to the `external_call` extension and integrate seamlessly with existing code.
- Reuse existing conventions and patterns established in the codebase.
- Avoid introducing additional dependencies unless absolutely necessary.

## ExecPlans

When explicitly asked, use an ExecPlan, as described in `.agent/PLANS.md`, from design to implementation.
