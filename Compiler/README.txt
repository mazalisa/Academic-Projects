Compiler implementation written in Ocaml for Scheme.
The procedure code_gen takes an e : expr'? and returns for it a string that contains lines of assembly instructions in the CISC architecture.
The procedure compile_scheme_file takes the name of a Scheme source file (e.g., foo.scm), and the name of a CISC assembly target file (e.g., foo.c).
Used Languages for the compiler: Ocaml, Scheme, C, CISC (Version of assembly language).