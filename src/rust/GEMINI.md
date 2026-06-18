# Rust Coding Guidelines & Conventions

This document defines the code structure, quality standards, and design patterns for Rust code in this repository. All contributors and AI assistants must follow these rules.

---

## 1. File & Project Headers
- **Copyright Header**: Every new source file must begin with the standard Apache 2.0 license copyright header.
- **Copyright Year**: The copyright year in the header must match the year the file was first created (e.g. `2026`). Do NOT update or modify the copyright year when modifying existing files.

---

## 2. Code Structure, Modularity & Method Complexity
- **Cohesive Files**: Favor splitting code across multiple small, highly focused source files rather than creating large monolithic files.
- **Subdirectories**: Group related files into logical subfolders.
- **Clean Imports**: Always place `use` import statements at the top of the file.
- **Visibility & Scoping**: Restrict types, functions, and methods to the narrowest possible visibility scope.
- **Method Length**: Keep methods and functions short, focused, and single-purpose. A function should ideally not exceed 40 lines of code.
- **Nesting Limits**: Avoid deep nesting (e.g., loops containing matches containing multi-line blocks). If a method has more than 2 levels of control nesting, extract the inner logic into descriptive private helper methods.
- **Responsibility Extraction**: Always extract distinct protocol steps (like reading startup packets, completing handshakes, or formatting status parameter blocks) into dedicated private methods rather than embedding them inside a master loop.



---

## 3. Naming Conventions & Variable Names
- **Descriptive Naming**: Use full, descriptive words instead of abbreviations for variable, parameter, type, and function names.
- **Allowed Abbreviations**: Only very common and universally understood abbreviations are permitted (e.g., `db` for database).
- **Disallowed Abbreviations**: Avoid short-hands such as `src` (use `source`), `dst` (use `destination`), `buf` (use `buffer`), `rs` (use `result_set`), `msg` (use `message`), `cc` (use `command_complete`), `err` (use `error`), etc.

---

## 4. Code Formatting & Formatting Checks
- **Rust Auto-Formatting**: Always run `cargo fmt` after making code changes to ensure format consistency across files.
- **Rust Code Cleanups**: Do not leave unnecessary or consecutive empty lines in Rust files. Ensure all files are clean of formatting discrepancies and compile cleanly as part of the post-modification cleanup steps.
- **TOML Auto-Formatting**: Always use `taplo fmt` to format all `Cargo.toml` configurations. Key entries under tables and dependency declarations should be aligned (configured via the local `.taplo.toml` file).
- **Linting**: Run `cargo clippy` and address any warnings before submitting code changes.

---

## 5. Self-Review & Refactoring
- **Complexity Review**: After writing code, perform a self-review to identify opportunities for simplification.
- **Code Simplification**: Refactor long or complex methods into shorter, single-responsibility functions. Reduce file size by modularizing code further if a file exceeds a few hundred lines.
- **Guideline Compliance Check**: Explicitly verify that all formatting, headers, naming, import structure, and error handling rules defined in this document are strictly followed.


---

## 6. Testing Strategy
- **Unit Test Coverage**: All crate-private or public functions, message encoders, and decoders must be covered by comprehensive unit tests. Unit tests must cover all execution paths, conditional branches (e.g. `if`, `else`, `match` arms), error states, and both states of optional fields (`Some` and `None`).
- **Location**: Place unit tests inside a nested `tests` module at the bottom of the source file (e.g. `#[cfg(test)] mod tests`).
- **Clear Assertions**: Prefer `expect("error message")` over `unwrap()` in tests, or provide clear assertion error messages.


---

## 7. Additional Rust Best Practices

### A. Fallible Conversions & Structured Errors
- Use `thiserror` to define custom, structured enum errors for parser or wire protocol failures instead of generic string errors (e.g., `anyhow::anyhow!("...")`). This makes error handling typed and robust.

### B. Panics & Unwrap Avoidance
- Do **NOT** use `.unwrap()` in library or application code. Use error propagation with `?` or handle errors gracefully.
- If a panic is technically impossible but a type check is required, use `.expect("descriptive message detailing why this is unreachable")`.
- In unit tests, prefer using `.expect("...")` rather than `.unwrap()` to provide meaningful debug context upon failure.

### C. Buffer & Bytes Management
- Leverage the `bytes::Bytes` and `bytes::BytesMut` interfaces for zero-copy buffer slicing and manipulation, minimizing unnecessary allocations and copies of binary payloads.

---

## 8. PostgreSQL Wire Protocol Compliance
- **Specification Alignment**: All frontend and backend message data structures and serialization/deserialization logic must strictly align with the official PostgreSQL Wire Protocol message formats (see https://www.postgresql.org/docs/current/protocol-message-formats.html).
- **Type Identifiers**: Define single-character message type code bytes as static constants in their respective message files (e.g., `pub(crate) const QUERY_IDENTIFIER: u8 = b'Q';`) and refer to these constants in parser dispatches and encoders instead of using raw literals.
- **Extensible Fields**: For messages with extensible or dynamic fields (such as `ErrorResponse` and `NoticeResponse`), represent the payload as an ordered list of key-value pairs (e.g. `Vec<(u8, String)>`) rather than a fixed set of struct fields, to accommodate custom server parameters and future protocol enhancements.


---

## 9. Performance & Zero-Copy Optimization
- **Zero-Copy Serialization**: Avoid unnecessary memory allocations, copying, and cloning during message serialization and deserialization.
- **Reference Lifetimes**: Prefer using borrowing and reference lifetimes (`&'a str`, `&'a [u8]`) rather than owned types (`String`, `Vec<u8>`) for outgoing message payloads, as these messages are constructed on-the-fly and immediately serialized to the connection buffer.
- **In-Place Buffer Modification**: When updating length fields or placeholders in byte buffers, write the values directly into the target slice (e.g., `destination[start_pos..start_pos + 4].copy_from_slice(&len.to_be_bytes())` which is optimized into a single instruction by the compiler) without allocating additional vectors or buffers.


