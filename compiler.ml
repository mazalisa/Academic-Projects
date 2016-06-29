(* compiler.ml
 * A compiler from Scheme to CISC
 *
 * Programmers: Mor Yaffe, Asaf Mazali, 2015
 *)

#use "pc.ml";;

exception X_not_yet_implemented;;
exception X_this_should_not_happen;;
let rec ormap f s =
  match s with
  | [] -> false
  | car :: cdr -> (f car) || (ormap f cdr);;

let rec andmap f s =
  match s with
  | [] -> true
  | car :: cdr -> (f car) && (andmap f cdr);;	  
let string_to_list str =
  let rec loop i limit =
    if i = limit then []
    else (String.get str i) :: (loop (i + 1) limit)
  in
  loop 0 (String.length str);;


let list_to_string s =
  let rec loop s n =
    match s with
    | [] -> String.make n '?'
    | car :: cdr ->
       let result = loop cdr (n + 1) in
       String.set result n car;
       result
  in
  loop s 0;;

type fraction = {numerator : int; denominator : int};;

type number =
  | Int of int
  | Fraction of fraction;;

type sexpr =
  | Void
  | Bool of bool
  | Nil
  | Number of number
  | Char of char
  | String of string
  | Symbol of string
  | Pair of sexpr * sexpr
  | Vector of sexpr list;;

module type SEXPR = sig
  val sexpr_to_string : sexpr -> string
end;; (* signature SEXPR *)

module Sexpr : SEXPR = struct
  
exception X_invalid_fraction of fraction;;


let normalize_scheme_symbol str =
  let s = string_to_list str in
  if (andmap
	(fun ch -> (ch = (Char.lowercase ch)))
	s) then str
  else Printf.sprintf "|%s|" str;;


let sexpr_to_string sexpr = 	
  let rec check_sexpr sexpr =
    (match sexpr with
     | Bool(x) -> (match x with
		   | true -> "#t"
		   | false -> "#f")
     | Number (x) -> (match x with
		      | Int(y) -> string_of_int y
		      | Fraction(y) -> (string_of_int y.numerator) 
				       ^ "/" 
				       ^ (string_of_int y.denominator))
     | Char (x) -> (match x with
		    | '\n' -> "#\\newline"
		    | '\r' -> "#\\return"
		    | '\t' -> "#\\tab"
		    | '\012' -> "#\\page"
		    | ' ' -> "#\\space"	
		    | _ -> "#\\" ^ (list_to_string [x]))
     | String (x) -> "\"" ^ x ^ "\""
     | Symbol (x) -> x
     | Pair(Symbol("quote"), Pair(Symbol(x),Nil)) -> "'" ^ x
     | Pair(Symbol("quasiquote"), Pair(Symbol(x),Nil)) -> "`" ^ x
     | Pair(Symbol("unquote-splicing"), Pair(Symbol(x),Nil)) -> ",@" ^ x
     | Pair(Symbol("unquote"), Pair(Symbol(x),Nil)) -> "," ^ x   
     | Pair (x,y) -> 
	let lst = pair_list sexpr in
	let str = (List.map check_sexpr lst) in
	"(" ^  (String.concat " " str)  ^ ")"
     | Vector (x) -> "#(" ^ String.concat " " 
			 (List.map check_sexpr x)
			  ^ ")"
     | _ -> raise X_this_should_not_happen)

and pair_list sexpr = 
    (match sexpr with
    | Nil -> []
    | Pair (x,y) -> List.append [x] (pair_list y)
    | sexpr -> [sexpr])

      in
    (check_sexpr sexpr)  
;;

end;; (* struct Sexpr *)

module type PARSER = sig
  val read_sexpr : string -> sexpr
  val read_sexprs : string -> sexpr list
end;;

module Parser : PARSER = struct

open PC;;

(* 3.2.1 *)
let nt_boolean =
  let nt1 = word_ci ("#t") in
  let nt1 = pack nt1 (fun _ -> Bool true) in
  let nt2 = word_ci ("#f") in
  let nt2 = pack nt2 (fun _ -> Bool false) in
  let nt = disj nt1 nt2 in
  nt
;;

(* 3.2.2 *)
let make_char_value base_char displacement =
  let base_char_value = Char.code base_char in
  fun ch -> (Char.code ch) - base_char_value + displacement;;

let nt_digit_0_9 = pack (range '0' '9') (make_char_value '0' 0);;
  
let nt_digit_1_9 = pack (range '0' '9') (make_char_value '0' 0);;
  
let nt_nat =
  let nt = range '1' '9' in
  let nt = pack nt (make_char_value '0' 0) in
  let nt' = range '0' '9' in
  let nt' = pack nt' (make_char_value '0' 0) in
  let nt' = star nt' in
  let nt = caten nt nt' in
  let nt = pack nt (fun (d, ds) -> (d :: ds)) in
  let nt = pack nt (fun s -> List.fold_left (fun a b -> a * 10 + b) 0 s) in
  let nt' = char '0' in
  let nt'' = char '0' in
  let nt''' = range '0' '9' in
  let nt'' = caten nt'' nt''' in
  let nt' = diff nt' nt'' in
  let nt' = pack nt' (fun e -> 0) in
  let nt = disj nt nt' in
  nt;;

let nt_nat_hex =
    let nt2 = range '0' '9' in
    let nt3 = range_ci 'A' 'F' in
    let nt4 = (plus (disj nt3 nt2)) in
    let nt = pack nt4 (fun a ->  int_of_string (list_to_string (['0';'x']@a))) in
    nt;;

let nt_int_dec =
  let nt = char '-' in
  let nt = pack nt (fun e -> -1) in
  let nt' = char '+' in
  let nt' = pack nt' (fun e -> 1) in
  let nt = disj nt nt' in
  let nt = maybe nt in
  let nt = pack nt (function | None -> 1 | Some(mult) -> mult) in
  
  let nt' = range '0' '9' in
  let nt' = pack nt' (make_char_value '0' 0) in
  let nt' = plus nt' in
  let nt' = pack nt' (fun s -> List.fold_left (fun a b -> a * 10 + b) 0 s) in

  let nt = caten nt nt' in 
  let nt = pack nt (fun (mult, n) -> (mult * n)) in
 nt;;

let nt_int_hex_minus = 
   (* let nt = char '+' in*)
    let nt1 = word_ci "-0x" in
    let nt2 = range '0' '9' in
    let nt3 = range_ci 'A' 'F' in
    let nt4 = (plus (disj nt3 nt2)) in
    let nt4 = caten nt1 nt4 in
    let nt = pack nt4 (fun (a, b) ->  int_of_string (list_to_string (a@b))) in
    nt;;

let nt_int_hex_none = 
   (* let nt = char '+' in*)
    let nt1 = word_ci "0x" in
    let nt2 = range '0' '9' in
    let nt3 = range_ci 'A' 'F' in
    let nt4 = (plus (disj nt3 nt2)) in
    let nt4 = caten nt1 nt4 in
    let nt = pack nt4 (fun (a, b) ->  int_of_string (list_to_string (a@b))) in
    nt;;

let nt_int_hex_plus = 
    let nt = char '+' in
    let nt1 = word_ci "0x" in
    let nt1 = caten nt nt1 in
    let nt1 = pack nt1 (fun (a, b) -> b) in
    let nt2 = range '0' '9' in
    let nt3 = range_ci 'A' 'F' in
    let nt4 = (plus (disj nt3 nt2)) in
    let nt4 = caten nt1 nt4 in
    let nt = pack nt4 (fun (a, b) ->  int_of_string (list_to_string (a@b))) in
    nt;;

let nt_int_hex = (disj nt_int_hex_none (disj nt_int_hex_plus nt_int_hex_minus));;
let nt_int = disj nt_int_hex nt_int_dec;;

(* 3.1.3 *)
let nt_slash =
    const (fun ch -> ch = '/')
;;
(* gcd function *)
let rec gcd a b =
    if b=0 then a else gcd b (a mod b);;

let nt_fraction =  
  let nt = caten (caten nt_int_dec nt_slash) nt_nat in
  let nt1 = caten (caten nt_int_hex nt_slash) nt_nat_hex in
  let nt = disj nt nt1 in
  let nt = pack nt (fun ((a, b), c) -> 
		     let d = gcd a c in
		     let d = abs d in
		     if c=0 then raise X_no_match
		     else if a=c then (Int 1)
		     else if d=c then (Int (a/c))
		     else if d>1
		     then Fraction ({numerator = a/d ; 
				     denominator = c/d})
		     else Fraction ({numerator = a ; denominator = c})) in
nt;;

let nt_number =
  let nt_num_int = pack nt_int (fun num -> Number (Int num)) in
  let nt_num_fraction = pack nt_fraction (fun num -> Number num) in
  disj nt_num_fraction nt_num_int;;

(* 3.2.3 *)
let nt_symbol =
  let nt = range_ci 'A' 'Z' in
  let nt0 = range '0' '9' in
  let nt1 = char '!' in
  let nt2 = char '$' in
  let nt3 = char '^' in
  let nt4 = char '*' in
  let nt5 = char '-' in
  let nt6 = char '_' in
  let nt7 = char '=' in
  let nt8 = char '+' in
  let nt9 = char '<' in
  let nt10 = char '>' in
  let nt11 = char '/' in
  let nt12 = char '?' in
  let nt = (disj nt (disj nt0 (disj nt1 (disj nt2 (disj nt3 (disj nt4 (disj nt5 (disj nt6 (disj nt7 (disj nt8 (disj nt9 (disj nt10 (disj nt11 nt12))))))))))))) in
   (plus nt);;

let nt_symbol_expr = 
  let nt = pack nt_symbol
		(fun b -> Symbol (String.lowercase (list_to_string b))) in 
  nt;;

let nt_parse_symbols =
    let nt = disj nt_number nt_symbol_expr in
 nt;;

(* Generic valid input check *)
let nt_valid s =
  let (_,rst) = nt_parse_symbols s in
    match rst with 
	| [] -> (nt_parse_symbols s)
	| car::cdr ->
	  match car with 
	  | ' ' -> (nt_parse_symbols s)
	  | ')' -> (nt_parse_symbols s)
	  | _ ->  (nt_symbol_expr s)
 ;;

(* 3.2.4 *)
let nt_string =
  let nt = char '"' in
  let nt_newline = word "\\n" in
  let nt_newline = pack nt_newline (fun _ -> '\n') in
  let nt_return = word "\\r" in
  let nt_return = pack nt_return (fun _ -> '\r') in
  let nt_tab = word "\\t" in
  let nt_tab = pack nt_tab (fun _ -> '\t') in
  let nt_backslash = word "\\\\" in
  let nt_backslash = pack nt_backslash (fun _ -> '\\') in
  let nt_dqoute = word "\\\"" in
  let nt_dqoute = pack nt_dqoute (fun _ -> '\"') in
  let nt_page = word "\\f" in
  let nt_page = pack nt_page (fun _ -> (Char.chr 12)) in
  let nt_meta = (disj nt_newline (disj nt_return (disj nt_tab 
		(disj nt_dqoute (disj nt_page nt_backslash))))) in
  let nt_chars = range (Char.chr 0) (Char.chr 127) in
  let nt_chars =  guard nt_chars (fun a -> a!='"') in
  let nt1 = star (disj nt_meta nt_chars) in
  let nt2 = caten nt nt1 in
  let nt2 = pack nt2 (fun (a,b) -> b) in
  let nt1 = caten nt2 nt in
  let nt = pack nt1 (fun (a,b) -> String (list_to_string a)) in
  nt;;

(* 3.2.5 *)
let nt_named_char =
    let nt1 = char '#' in
    let nt2 = word "\\" in
    let nt = caten nt1 nt2 in 
    let nt1 = word_ci "newline" in
    let nt2 = caten nt nt1 in
    let nt2 = pack nt2 (fun _ -> (Char '\n')) in
    let nt3 = word_ci "return" in
    let nt3 = caten nt nt3 in
    let nt3 = pack nt3 (fun _ -> (Char (Char.chr 13))) in
    let nt4 = word_ci "tab" in
    let nt4 = caten nt nt4 in
    let nt4 = pack nt4 (fun _ -> (Char (Char.chr 9))) in
    let nt5 = word_ci "page" in
    let nt5 = caten nt nt5 in
    let nt5 = pack nt5 (fun _ -> (Char (Char.chr 12))) in
    let nt6 = word_ci "space" in
    let nt6 = caten nt nt6 in
    let nt6 = pack nt6 (fun _ -> (Char (Char.chr 32))) in
    let nt = (disj nt2 (disj nt3 (disj nt4 (disj nt5 nt6)))) in
    nt;;

let nt_visible_char =
    let nt1 = char '#' in
    let nt2 = char (Char.chr 92) in
    let nt = caten nt1 nt2 in
    let nt1 = range ' ' '~' in
    let nt2 = caten nt nt1 in
    let nt = pack nt2 (fun (a, b) -> (Char b)) in
    nt;;

let nt_char = disj nt_named_char nt_visible_char;;
 
(* 3.2.7 *)	  
let rec nt_pair s =
  let nt = (disj nt_proper_lst nt_improper_lst) in
  let nt = pack nt (fun (s,e) ->
		       List.fold_right
			 (fun a b -> Pair(a,b))
			 s
			 e) in
  nt s

and nt_proper_lst s =
  let nt = caten (char '(') (star nt_sexpr) in
  let nt = pack nt (fun (_,b) -> b) in
  let nt1 = char ')' in
  let nt1 = pack nt1 (fun _ -> Nil) in
  let nt = caten nt nt1 in
  nt s

and nt_improper_lst s =
  let nt = caten (char '(') (plus nt_sexpr) in
  let nt = pack nt (fun (_,b) -> b) in
  let nt1 = char '.' in
  let nt2 = nt_sexpr in
  let nt3 = char ')' in
  let nt1 = caten nt1 (caten nt2 nt3) in
  let nt1 = pack nt1 (fun (_,(b,_)) -> b) in
  let nt = caten nt nt1 in
  nt s

(* 3.2.8 *)
and nt_vector s =
  let nt1 = word "#(" in
  let nt2 = star nt_sexpr in
  let nt1 = caten nt1 nt2 in
  let nt1 = pack nt1 (fun (_,s) -> s) in
  let nt2 = char ')' in
  let nt1 = caten nt1 nt2 in
  let nt1 = pack nt1 (fun (a,_) -> Vector a) in
  nt1 s

(* 3.2.9 *)
and nt_quote_quote s =
  let nt = word "'" in
  let nt1 = word_ci "quote" in
  let nt = disj nt nt1 in
  let nt = pack nt (fun _ -> (Symbol "quote")) in
  let nt2 = nt_sexpr in
  let nt = caten nt nt2 in
  let nt = pack nt (fun (a,b) -> Pair(a,(Pair (b, Nil)))) in
  nt s

and nt_quote_quasiquote s =
  let nt = word "`" in
  let nt1 = word_ci "quasiquote" in
  let nt = disj nt nt1 in
  let nt = pack nt (fun _ -> (Symbol "quasiquote")) in
  let nt2 = nt_sexpr in
  let nt = caten nt nt2 in
  let nt = pack nt (fun (a,b) -> Pair(a,(Pair (b, Nil)))) in
  nt s

and nt_quote_unquote_splicing s =
  let nt = word ",@" in
  let nt1 = word_ci "unquote-splicing" in
  let nt = disj nt nt1 in
  let nt = pack nt (fun _ -> (Symbol "unquote-splicing")) in
  let nt2 = nt_sexpr in
  let nt = caten nt nt2 in
  let nt = pack nt (fun (a,b) -> Pair(a,(Pair (b, Nil)))) in
  nt s

and nt_quote_unquote s =
  let nt = word "," in
  let nt1 = word_ci "unquote" in
  let nt = disj nt nt1 in
  let nt = pack nt (fun _ -> (Symbol "unquote")) in
  let nt2 = nt_sexpr in
  let nt = caten nt nt2 in
  let nt = pack nt (fun (a,b) -> Pair(a,(Pair (b, Nil)))) in
  nt s

and nt_quote_like s =
  (disj_list [nt_quote_quote; nt_quote_quasiquote; 
	      nt_quote_unquote_splicing; nt_quote_unquote]) s


and nt_sexpr_comments s =
  let nt2 = word "#;" in
  let nt1 = caten nt2 nt_sexpr in
nt1 s

and nt_line_comments s =
  let nt_ignore_chars = guard nt_any (fun c -> c!='\n') in
  let nt_ignore_chars = star nt_ignore_chars in
  let nt = char ';' in
  let nt = caten nt nt_ignore_chars in
  nt s

(* 3.2.6 *)
and nt_nil s =
let a = char '(' in
let b = star ignore_comments in
let c = char ')' in
let nt = caten a (caten b c) in
let nt = pack nt (fun (a, (b, c)) -> Nil) in
nt s

and ignore_comments s =  disj_list [(pack nt_sexpr_comments (fun _ -> true));
				   (pack nt_line_comments (fun _-> true));
				   (pack nt_whitespace (fun _ -> true))]
				   s

and wrap_comments nt s =
  let nt1 = star ignore_comments in
  let nt1 = caten nt1 (caten nt nt1) in
  let nt = pack nt1 (fun (a, (b, c)) -> b) in
  nt s

and nt_sexpr s = (disj_list [(wrap_comments nt_nil);
			     (wrap_comments nt_boolean); 
			     (wrap_comments nt_pair);
			     (wrap_comments nt_nil);
			     (wrap_comments nt_valid); 
			     (wrap_comments nt_string); 
			     (wrap_comments nt_char); 
			     (wrap_comments nt_vector);
			     (wrap_comments nt_quote_like)])
  s;;

(* Generic valid input check *)
let nt_valid_sexpr s =
  let (_,rst) = nt_sexpr s in
    match rst with 
	| [] -> (nt_sexpr s)
	| _ -> raise X_no_match
 ;;

let nt_valid_sexprs s =
  let (_,rst) = (star nt_sexpr) s in
    match rst with 
	| [] -> (star nt_sexpr) s
	| _ -> raise X_no_match
 ;;

let read_sexpr string = 
  let lst = (string_to_list string) in
  let (a,_) = nt_valid_sexpr lst in
  a;;

let read_sexprs string = 
     let lst = (string_to_list string) in
     let (a,_) = nt_valid_sexprs lst in
     a;;

end;; (* struct Parser *)

(* work on the tag parser starts here *)

type expr =
  | Const of sexpr
  | Var of string
  | If of expr * expr * expr
  | Seq of expr list
  | Set of expr * expr
  | Def of expr * expr
  | Or of expr list
  | LambdaSimple of string list * expr
  | LambdaOpt of string list * string * expr
  | Applic of expr * (expr list);;

exception X_syntax_error;;

module type TAG_PARSER = sig
  val read_expression : string -> expr
  val read_expressions : string -> expr list
  val expression_to_string : expr -> string
end;; (* signature TAG_PARSER *)

module Tag_Parser : TAG_PARSER = struct

let reserved_word_list =
  ["and"; "begin"; "cond"; "define"; "do"; "else";
   "if"; "lambda"; "let"; "let*"; "letrec"; "or";
   "quasiquote"; "quote"; "set!"; "unquote";
   "unquote-splicing"];;  

let rec process_scheme_list s ret_nil ret_one ret_several =
  match s with
  | Nil -> ret_nil ()
  | (Pair(sexpr, sexprs)) ->
     process_scheme_list sexprs
			 (fun () -> ret_one sexpr)
			 (fun sexpr' -> ret_several [sexpr; sexpr'])
			 (fun sexprs -> ret_several (sexpr :: sexprs))
  | _ -> raise X_syntax_error;;
  
let scheme_list_to_ocaml_list args = 
  process_scheme_list args
		      (fun () -> [])
		      (fun sexpr -> [sexpr])
		      (fun sexprs -> sexprs);;

(************* Handles Nil ******************)
let rec process_scheme_list2 s ret_one ret_several =
  match s with
  | (Pair(sexpr, sexprs)) ->
     process_scheme_list2 sexprs
			 (fun sexpr' -> ret_several [sexpr; sexpr'])
			 (fun sexprs -> ret_several (sexpr :: sexprs))
  | sexpr -> ret_one sexpr;;
  
let scheme_list_to_ocaml_list2 args = 
  process_scheme_list2 args
		      (fun sexpr -> [sexpr])
		      (fun sexprs -> sexprs);;
(********************************************)    
let expand_let_star ribs sexprs =
  let ribs = scheme_list_to_ocaml_list ribs in
  let params = List.map (function
			  | (Pair(name, (Pair(expr, Nil)))) -> name
			  | _ -> raise X_this_should_not_happen) ribs in
  let args = List.map
	       (function
		 | (Pair(name, (Pair(expr, Nil)))) -> expr
		 | _ -> raise X_this_should_not_happen) ribs in
  let params_set = List.fold_right
		     (fun a s ->
		      if (ormap
			    (fun b ->
			     (match (a, b) with
			      | (Symbol a, Symbol b) -> a = b
			      | _ -> raise X_this_should_not_happen))
			    s)
		      then s else a :: s)
		     params
		     [] in
  let place_holders = List.fold_right
			(fun a s -> Pair(a, s))
			(List.map
			   (fun var -> (Pair(var, (Pair((Bool false), Nil)))))
			   params_set)
			Nil in
  let assignments = List.map2
		      (fun var expr ->
		       (Pair((Symbol("set!")),
			     (Pair(var, (Pair(expr, Nil)))))))
		      params args in
  let body = List.fold_right
	       (fun a s -> Pair(a, s))
	       assignments
	       sexprs in
  (Pair((Symbol("let")), (Pair(place_holders, body))));;

let expand_letrec ribs sexprs =
  let ribs = scheme_list_to_ocaml_list ribs in
  let params = List.map (function
			  | (Pair(name, (Pair(expr, Nil)))) -> name
			  | _ -> raise X_this_should_not_happen) ribs in
  let args = List.map
	       (function
		 | (Pair(name, (Pair(expr, Nil)))) -> expr
		 | _ -> raise X_this_should_not_happen) ribs in
  let ribs = List.map
	       (function
		 | (Pair(name, (Pair(expr, Nil)))) ->
		    (Pair(name, (Pair(Bool false, Nil))))
		 | _ -> raise X_this_should_not_happen)
	       ribs in
  let body = List.fold_right
	       (fun a s -> Pair(a, s))
	       (List.map2
		  (fun var expr ->
		   (Pair((Symbol("set!")),
			 (Pair(var, (Pair(expr, Nil)))))))
		  params args)
	       sexprs in
  let ribs = List.fold_right
	       (fun a s -> Pair(a, s))
	       ribs
	       Nil in
  (Pair((Symbol("let")), (Pair(ribs, body))));;

exception X_unquote_splicing_here_makes_no_sense;;

let rec expand_qq sexpr = match sexpr with
  | (Pair((Symbol("unquote")), (Pair(sexpr, Nil)))) -> sexpr
  | (Pair((Symbol("unquote-splicing")), (Pair(sexpr, Nil)))) ->
     raise X_unquote_splicing_here_makes_no_sense
  | (Pair(a, b)) ->
     (match (a, b) with
      | ((Pair((Symbol("unquote-splicing")), (Pair(a, Nil)))), b) ->
	 let b = expand_qq b in
	 (Pair((Symbol("append")),
	       (Pair(a, (Pair(b, Nil))))))
      | (a, (Pair((Symbol("unquote-splicing")), (Pair(b, Nil))))) ->
	 let a = expand_qq a in
	 (Pair((Symbol("cons")), (Pair(a, (Pair(b, Nil))))))
      | (a, b) ->
	 let a = expand_qq a in
	 let b = expand_qq b in
	 (Pair((Symbol("cons")), (Pair(a, (Pair(b, Nil)))))))
  | (Vector(sexprs)) ->
     let s = expand_qq (List.fold_right (fun a b -> Pair(a, b)) sexprs Nil) in
     (Pair((Symbol("list->vector")), (Pair(s, Nil))))
  | Nil | Symbol _ -> (Pair((Symbol("quote")), (Pair(sexpr, Nil))))
  | expr -> expr;;

let tag_parse sexpr = 
  let rec tag_parse_rec sexpr =
    (match sexpr with
     | Void -> Const(Void)
     | Bool(_) | Char(_) | Number(_) | String(_) | Vector(_) -> Const sexpr
     | Symbol(x) ->
	    if (not (List.mem x reserved_word_list)) then Var x
            else raise X_syntax_error
     | Pair((Symbol "quote"), Pair (x, Nil)) -> Const x
     | Pair((Symbol "quasiquote"), Pair (x, Nil)) -> (tag_parse_rec (expand_qq x))
     | Pair((Symbol "unquote"), Pair (_, Nil)) -> raise X_syntax_error
     | Pair((Symbol "unquote-splicing"), Pair (_, Nil)) -> raise X_syntax_error
     | Pair(Symbol(x), exp) -> (match x with
				| "if" -> if_handle exp
			        | "cond" -> cond_handle exp
				| "lambda" -> lambda_handle exp
				| "or" -> or_handle exp
				| "define" -> define_handle exp
				| "set!" -> set_handle exp
			        | "and" -> and_handle exp
				| "begin" -> begin_handle exp
				| "let" -> let_handle exp
				| "let*" -> let_star_handle exp
				| "letrec" -> letrec_handle exp
		                | _ -> (applic_handle (Symbol x) exp))
     | Pair (Pair(Symbol "lambda", exp), args) ->
	(applic_handle (Pair(Symbol "lambda", exp)) args)
     | Pair(x, Nil) -> (tag_parse_rec x)
     | Pair(op, exp) -> (applic_handle op exp)
     | _ -> raise X_syntax_error) 
 					    
and if_handle exp =
  (match exp with
   |  Pair(first, Pair(second, Pair(third, Nil))) ->
       let first =  (tag_parse_rec first) in
       let second =  (tag_parse_rec second) in
       let third = (tag_parse_rec third) in
       If(first,second,third)
   |  Pair(first, Pair(second, Nil)) ->
       let first =  (tag_parse_rec first) in
       let second =  (tag_parse_rec second) in
       If(first, second, Const Void)
   | _ -> raise X_syntax_error)

and letrec_handle exp =
  match exp with 
  | Pair(ribs,sexprs) ->
       (match ribs with
	| Pair(_,_) | Nil ->
	   (tag_parse_rec (expand_letrec ribs sexprs))
	| _ -> raise X_syntax_error)
  | _ -> raise X_syntax_error

and let_star_handle exp =
  match exp with 
  | Pair(ribs,sexprs) ->
     (match ribs with
      | Pair(_,_) | Nil ->
	 (tag_parse_rec (expand_let_star ribs sexprs))
      | _ -> raise X_syntax_error)
  | _ -> raise X_syntax_error

and lft pair =
  match pair with
  | Pair(x,y) -> x
  | _ -> raise X_this_should_not_happen

and rht pair =
  match pair with
  | Pair(x,y) -> y
  | _ -> raise X_this_should_not_happen

and bindings_params exp =
  let params = scheme_list_to_ocaml_list exp in
  let params = List.map (fun x -> lft x) params in
  let params = List.map (fun x -> 
			  (match x with 
			   | Symbol(y) -> y
			   | _ -> raise X_this_should_not_happen)) params in
  params
  
and bindings_args exp =      
  let args = scheme_list_to_ocaml_list exp in
  let args = List.map (fun x -> rht x) args in
  let args = (List.map (fun x -> (tag_parse_rec x)) args) in
  args

and let_handle exp =
  (match exp with
   | Pair(bindings, body) ->
      (match bindings with
       | Pair(_,_) | Nil ->
	  let params = (bindings_params bindings) in
	  let args = (bindings_args bindings) in
	  let body = (body_handle body) in
	  let lambd = LambdaSimple(params, body) in
	  Applic(lambd, args)
       | _ -> raise X_syntax_error)
  | _ -> raise X_syntax_error)
      
and begin_handle exp =
  (match exp with 
   | Nil -> Const Void
   | Pair (x, Nil) -> (tag_parse_rec x)
   | _ -> let rest = scheme_list_to_ocaml_list exp in
	  let rest = (List.map (fun x -> (tag_parse_rec x)) rest) in
	  Seq (rest))

and and_handle exp =
  (match exp with 
   | Nil -> Const (Bool true)
   | Pair(x, Nil) -> (tag_parse_rec x)
   | Pair(x, cont) -> 
      let conti = (and_handle cont) in 
      let x_parse = (tag_parse_rec x) in
      If (x_parse, conti, Const (Bool false)) 
   | _ -> raise X_syntax_error)

and set_handle exp =
  (match exp with 
   | Pair(Symbol(name), rest) ->
      let rest = scheme_list_to_ocaml_list rest in
      if ((List.length rest) > 1) then raise X_syntax_error
      else if ((List.length rest) = 0) then raise X_syntax_error
      else let rest = tag_parse_rec (List.hd rest) in
	   let name = tag_parse_rec (Symbol(name)) in
	   Set(name, rest)
   | _ -> raise X_syntax_error)

and define_handle exp =
  (match exp with 
   | Pair(Symbol(name), rest) ->
      let rest = scheme_list_to_ocaml_list rest in
      if ((List.length rest) > 1) then raise X_syntax_error
      else if ((List.length rest) = 0) then raise X_syntax_error
      else let rest = tag_parse_rec (List.hd rest) in
	   let name = tag_parse_rec (Symbol(name)) in
	   Def(name, rest)
   | Pair(Pair(Symbol(name), params), body) -> 
      let parseName = tag_parse_rec (Symbol(name)) in
      let lambd = lambda_handle (Pair(params, body)) in
      Def (parseName, lambd)
   | _ -> raise X_syntax_error)

and or_handle exp =
   let args = scheme_list_to_ocaml_list exp in
   if ((List.length args) = 0) then Const (Bool(false))
   else if ((List.length args) = 1) then (tag_parse_rec (List.hd args))
   else let args = (List.map (fun x -> (tag_parse_rec x)) args) in
	Or(args)

and applic_handle x exp =
   let args = scheme_list_to_ocaml_list exp in
   let args = (List.map (fun x -> (tag_parse_rec x)) args) in
   let op = (tag_parse_rec x) in
   Applic(op, args)
  
and cond_handle exp =
  (match exp with
	| Pair(Pair(Symbol("else"), rest), cont) -> 
	  ( match rest with
	   | Nil -> raise X_syntax_error
	   | _ -> (tag_parse_rec rest))
	| _ -> helper exp)

and check_seq exp =
  let exp = scheme_list_to_ocaml_list exp in
  if ((List.length exp) = 1) then (tag_parse_rec (List.hd exp))
  else let exp = (List.map (fun x -> (tag_parse_rec x)) exp) in
  Seq(exp)

and helper exp =
  (match exp with 
        | Nil -> Const Void
        | Pair(Pair(first,Nil), cont) -> 
               If((tag_parse_rec first),(Const Void),(helper cont))
        | Pair(Pair(first, second), cont) ->
	   let first =  (tag_parse_rec first) in
	   let second = check_seq second in
	     (match cont with 
	      | Nil -> If(first, second, Const Void)
	      | Pair(Pair(Symbol "else",Pair(x, Nil)), Nil) -> 
	   	 let third = (tag_parse_rec x) in
		  If(first, second, third)
	      |  Pair(Pair(Symbol "else",Nil), Nil) -> 
		 let third = (Const Void) in
		 If(first, second, third)
	      | Pair(a,b) ->  
		 let third = (helper cont) in
		 If(first, second, third)
	      | _ -> raise X_this_should_not_happen)
       | _ -> raise X_syntax_error)

and lambda_handle exp =
  (match exp with
   | Nil -> raise X_syntax_error
   | Pair (Symbol(x), body) ->
      let body = (body_handle body) in
      LambdaOpt([], x, body)
   | Pair (params, body) ->
           let body = (body_handle body) in
	   let params = (List.map (fun (x) -> (match x with
				  | Symbol(y) -> y
				  | Nil -> "simple"
				  | _ -> raise X_this_should_not_happen)) 
				  (scheme_list_to_ocaml_list2 params)) in
	   if (List.mem "simple" params) then
	     let params = (List.filter (fun s -> not(s="simple")) params) in
	     LambdaSimple(params, body)
	   else
	     let last = function
	       | car::cdr -> List.fold_left (fun _ y -> y) car cdr
	       | []    -> raise X_this_should_not_happen in
	     let opt = (last params) in
	     let remove_last lst =
	       List.rev (List.tl (List.rev lst)) in
	     let params = remove_last params in
	     LambdaOpt(params, opt, body)
      | _ -> raise X_syntax_error)

and check_nested_define body =
  (match body with
   | Nil -> []
   | Pair(Pair(Symbol("define"),x), rest) -> 
       (match x with
	     | Pair (Pair(y, params), body) ->
		      let def_sexpr = Pair(y, Pair ((Pair((Symbol("lambda")), 
							    (Pair(params,body)))), Nil)) in
		       List.append [def_sexpr] (check_nested_define rest)
	     | _ -> List.append [x] (check_nested_define rest))
    | Pair(Pair(Symbol("begin"),x), rest) ->
            List.append (check_nested_define x) (check_nested_define rest)

    | _ -> [])

and get_rest body =
  (match body with
    | Nil -> []
    | Pair(Pair(Symbol("define"),x), rest) -> List.append (get_rest rest) []
    | Pair(Pair(Symbol("begin"),x), rest) ->
           List.append (get_rest x) (get_rest rest)
    | _ -> [body])

and body_handle_reg body_lst =

    if ((List.length body_lst) = 1) then
    (tag_parse_rec (List.hd body_lst))
    else let body = (List.map (fun x -> (tag_parse_rec x)) body_lst) in
       Seq(body)

and pair_it_up lst =
  match lst with
  | car :: cdr ->
     (match cdr with 
     | [] -> Pair(car, Nil)
     | _ -> Pair(car, (pair_it_up cdr)))
  | _ -> raise X_this_should_not_happen
	  

and body_handle body =
  let body1 = scheme_list_to_ocaml_list body in
  if ((List.length body1)) = 0 then (Const Void)
  else let defines = check_nested_define body in
  if ((List.length defines)) = 0 then body_handle_reg body1
  else let ribs = (pair_it_up defines) in
       let rest = (List.hd (get_rest body)) in
       tag_parse_rec (Pair(Symbol("letrec"), Pair(ribs,rest)))
  


  in (tag_parse_rec sexpr)
;;
    
let read_expression string = tag_parse (Parser.read_sexpr string);;

let read_expressions string = List.map tag_parse (Parser.read_sexprs string);;

let expression_to_string expr = 
  let rec check_expr expr =
    (match expr with 
     | Const(x) -> (match x with 
		    | Nil -> "'()"
		    | Void -> ""
		    | Bool(_)| Number(_) | Char(_) | String(_) -> (Sexpr.sexpr_to_string x)
		    | _ -> "'" ^ (Sexpr.sexpr_to_string x))
     | Var(x) -> x
     | If(pred, consq, alt) -> "(if " ^ (check_expr pred) ^ " " 
	                     ^ (check_expr consq) ^
			       if (alt=Const(Void)) then ")"
	                       else " " ^ (check_expr alt) ^ ")"
     | Or(lst) -> "(or " ^ String.concat " " 
			 (List.map check_expr lst) ^ ")"
     | Seq(lst) -> "(begin " ^ String.concat " " 
			 (List.map check_expr lst) ^ ")"
     | Set(var,value) -> "(set! " ^ (check_expr var) ^ 
			 " " ^ (check_expr value) ^ ")" 
     | Def(var,value) -> "(define " ^ (check_expr var) ^ 
			   " " ^ (check_expr value) ^ ")" 
     | LambdaSimple(params,body) -> "(lambda (" ^ String.concat " " 
			 (List.map (fun (x) -> x) params) ^ ")"
		         ^ if (body=Const(Void)) then " )"
		         else " " ^ (check_expr body) ^ ")"
     | LambdaOpt(params,opt,body) -> let body_string =  if (body=Const(Void)) then ")"
				    else (check_expr body) ^ ")" in
				    "(lambda " ^ if params = [] then opt ^ " " ^ body_string 
				    else "(" ^ String.concat " " 
			            (List.map (fun (x) -> x) params) ^ " . " ^
			            opt ^ ") " ^ body_string
     | Applic(op,args) -> "(" ^ (check_expr op) ^ 
			if args=[] then ")" else
			" " ^ String.concat " " 
			(List.map check_expr args) ^ ")")
      in
      (check_expr expr)
;;
 

end;; (* struct Tag_Parser *)

(* work on the semantics starts here *)
type var = 
  | VarFree' of string
  | VarParam' of string * int
  | VarBound' of string * int * int;;

type expr' =
  | Const' of sexpr
  | Var' of var
  | Box' of var
  | BoxGet' of var
  | BoxSet' of var * expr'
  | If' of expr' * expr' * expr'
  | Seq' of expr' list
  | Set' of expr' * expr'
  | Def' of expr' * expr'
  | Or' of expr' list
  | LambdaSimple' of string list * expr'
  | LambdaOpt' of string list * string * expr'
  | Applic' of expr' * (expr' list)
  | ApplicTP' of expr' * (expr' list);;

module type SEMANTICS = sig
  val run_semantics : expr -> expr'
  val annotate_lexical_addresses : expr -> expr'
  val annotate_tail_calls : expr' -> expr'
  val box_set : expr' -> expr'
end;;

module Semantics : SEMANTICS = struct

let calc_minor var params =
  let rec loop i params =
    if(var = (List.hd params)) then i		   
    else loop (i+1) (List.tl params) in
  (loop 0 params)
;;

let rec annotate_lexical_rec ast (scope : string list list) =
 let params = if (scope = []) then [] else (List.hd scope) in
  (match ast with
   | Const(sexpr)-> Const'(sexpr)
   | Var(var) -> Var'(if List.mem var params then VarParam'(var, (calc_minor var params))
		 else if (mem_scope var scope) then VarBound'(var,((calc_major var scope) - 1),
							     (calc_minor_from_scope var scope))
		 else VarFree'(var))
   | If(pred, consq, alt) ->  If'((annotate_lexical_rec pred scope), 
				  (annotate_lexical_rec consq scope), 
				  (annotate_lexical_rec alt scope))
   | Or(lst) -> Or'(List.map (fun x -> (annotate_lexical_rec x scope)) lst)
   | Seq(lst) -> Seq'(List.map (fun x -> (annotate_lexical_rec x scope)) lst)
   | Set(var,value) ->  Set'((annotate_lexical_rec var scope ), 
			    (annotate_lexical_rec value scope )) 
   | Def(var,value) -> Def'((annotate_lexical_rec var scope ), 
			    (annotate_lexical_rec value scope ))
   | LambdaSimple(lam_params,body) -> let scope1 = List.append [lam_params] scope in
				      LambdaSimple'(lam_params, (annotate_lexical_rec body scope1))
   | LambdaOpt(lam_params,opt,body) ->  let params1 = List.append lam_params [opt] in
					let scope1 = List.append [params1] scope in
				       LambdaOpt'(lam_params, opt, 
						  (annotate_lexical_rec body scope1))
   | Applic(op,args) -> Applic'((annotate_lexical_rec op scope), 
			    (List.map (fun x -> (annotate_lexical_rec x scope )) args)))

and calc_minor_from_scope var scope =
  let lst = (List.hd scope) in
  if (List.mem var lst) then (calc_minor var lst)
  else (calc_minor_from_scope var (List.tl scope))

and calc_major var scope =
  let rec loop i scope =
  let lst = (List.hd scope) in
  if (List.mem var lst) then i
  else (loop (i+1) (List.tl scope)) in
  (loop 0 scope)

and mem_scope var scope = ormap (fun lst -> List.mem var lst) scope 
;;

let annotate_lexical_addresses e = (annotate_lexical_rec e []);;

let rec annotate_tail_rec ast tp =
(match ast with
  | Const'(sexpr) as x -> x
  | Var'(var) as x -> x
  | If'(pred, consq, alt) -> If'((annotate_tail_rec pred false),
				 (annotate_tail_rec consq tp), 
				 (annotate_tail_rec alt tp))
  | Seq'(lst) -> Seq'(make_rec_lst lst tp)
  | Or'(lst) -> Or'(make_rec_lst lst tp)
  | Set'(var, value) -> Set'(var, (annotate_tail_rec value false))
  | Def'(var, value) -> Def'(var, (annotate_tail_rec value false))
  | LambdaSimple'(params, body) -> LambdaSimple'(params, (annotate_tail_rec body true))
  | LambdaOpt'(params, opt, body) -> LambdaOpt'(params, opt, (annotate_tail_rec body true))
  | Applic'(op, args) -> if (tp=true) then ApplicTP'((annotate_tail_rec op false),
						    (List.map (fun x -> 
							       (annotate_tail_rec x false))
							     args))
			 else Applic'((annotate_tail_rec op false),
						    (List.map (fun x -> 
							       (annotate_tail_rec x false))
							     args))
  | _ -> raise X_this_should_not_happen)

and  make_rec_lst lst tp =
		  match lst with
		  | [] -> []
		  | car::cdr -> if (cdr = []) then  [(annotate_tail_rec car tp)]
			       else List.append [(annotate_tail_rec car false)] 
						(make_rec_lst cdr tp)
;;

let annotate_tail_calls e = (annotate_tail_rec e false);;

let rec box_set_rec ast boxed_lst =
  (match ast with
   | Const'(sexpr) as x -> x
   | Box'(var) as x -> x
   | BoxGet'(var) as x -> x
   | BoxSet'(var, exp) as x -> x
   | Var'(var) -> (match var with
		   | VarFree'(var) as x -> Var'(x)
		   | VarParam'(var, minor) as x -> if (List.mem var boxed_lst) 
						   then BoxGet'(x) else Var'(x)
		   | VarBound'(var, major, minor) as x -> if (List.mem var boxed_lst) 
							  then BoxGet'(x) else Var'(x))
   | If'(pred, consq, alt) -> If'((box_set_rec pred boxed_lst),
				 (box_set_rec consq boxed_lst),
				 (box_set_rec alt boxed_lst))
   | Seq'(lst) -> Seq'(List.map (fun x -> (box_set_rec x boxed_lst)) lst)
   | Set'(variable, value) -> let box_value = (box_set_rec value boxed_lst) in
			      (match variable with 
			       | Var'(var) as y -> (match var with
					       | VarFree'(var) -> Set'(y, box_value)
					       | VarParam'(var, minor) as x -> 
						               if (List.mem var boxed_lst) 
							       then BoxSet'(x, box_value) 
							       else Set'(y, box_value)
					       | VarBound'(var, major, minor) as x -> 
				                               if (List.mem var boxed_lst)
							       then BoxSet'(x, box_value) 
							       else Set'(y, box_value))
			       | _ -> Set'((box_set_rec variable boxed_lst), 
				       box_value))
   | Def'(var, value) -> Def'((box_set_rec var boxed_lst), (box_set_rec value boxed_lst))
   | Or'(lst) -> Or'(List.map (fun x -> (box_set_rec x boxed_lst)) lst)
   | LambdaSimple'(params, body) as y -> if (body = Const'(Void)) then y
					 else let updated_boxed_lst = 
				                  (update_boxed_lst boxed_lst params body) in
				              let lambda_to_box = List.filter (fun x -> (List.mem x params))
						        updated_boxed_lst in
					      let body1 = List.append
						 (List.map (fun x -> 
							 let minor_x = (calc_minor x params) in
							 let param_x = VarParam'(x, minor_x) in
							 Set'(Var'(param_x), Box'(param_x)))
						  lambda_to_box)
						  [(box_set_rec body updated_boxed_lst)] in
				      if ((List.length body1) = 1) then
					LambdaSimple'(params, (List.hd body1))
	                              else LambdaSimple'(params, Seq'(body1))
   | LambdaOpt'(params, opt, body) as y -> if (body = Const'(Void)) then y
                                           else let params1 = List.append params [opt] in
                                                let updated_boxed_lst = 
				                    (update_boxed_lst boxed_lst params1 body) in
				                let lambda_to_box = List.filter 
							(fun x -> (List.mem x params1))
						        updated_boxed_lst in
				                let body1 = List.append
						    (List.map (fun x -> 
							 let minor_x = (calc_minor x params1) in
							 let param_x = VarParam'(x, minor_x) in
							 Set'(Var'(param_x), Box'(param_x)))
						  lambda_to_box)
						  [(box_set_rec body updated_boxed_lst)] in
				      if ((List.length body1) = 1) then
					LambdaOpt'(params, opt, (List.hd body1))
	                              else LambdaOpt'(params, opt, Seq'(body1))
   | Applic'(op, args) -> Applic'((box_set_rec op boxed_lst), 
				  (List.map (fun x -> (box_set_rec x boxed_lst)) args))
   | ApplicTP'(op, args) -> ApplicTP'((box_set_rec op boxed_lst), 
				  (List.map (fun x -> (box_set_rec x boxed_lst)) args)))

    and update_boxed_lst boxed_lst params body =
      let lst = List.filter (fun x -> (not (List.mem x params))) boxed_lst in
      List.append lst (List.filter (fun x -> (isBound x body) &&
					     (isSet x body) &&
					     (isGet x body)) params)

    and check_lst p lst func = 
       (match lst with
	| [] -> false
	| car :: cdr -> if (cdr = []) then  (func p car)
			       else (func p car) || (check_lst p cdr func))

   and isBound p body = 
      (match body with
       | Const'(_) | Box'(_) | BoxGet'(_) | BoxSet'(_) -> false
       | Var'(var) -> (match var with
		       | VarBound'(x, _, _) -> if (x = p) then true
		                               else false
		       | _ -> false)
       | If'(pred, consq, alt) -> (isBound p pred) ||
		                  (isBound p consq) ||
		                  (isBound p alt)
       | Seq'(lst) -> (check_lst p lst isBound)
       | Set'(var, value) -> (isBound p var) || (isBound p value)
       | Def'(var, value) -> (isBound p var) || (isBound p value)
       | Or'(lst) ->  (check_lst p lst isBound)
       | LambdaSimple'(params, body1) -> if (List.mem p params) then false
		                         else (isBound p body1)
       | LambdaOpt'(params, opt, body1) -> let params1 = List.append params [opt] in
	                                   if (List.mem p params1) then false
		                           else (isBound p body1)
       | Applic'(op, args) -> (isBound p op) || (check_lst p args isBound)
       | ApplicTP'(op, args) -> (isBound p op) || (check_lst p args isBound))

   and isSet p body = 
      (match body with
       | Const'(_) | Box'(_) | BoxGet'(_) | BoxSet'(_) | Var'(_) -> false
       | If'(pred, consq, alt) -> (isSet p pred) ||
		                  (isSet p consq) ||
		                  (isSet p alt)
       | Seq'(lst) -> (check_lst p lst isSet)
       | Set'(variable, value) -> (match variable with
			      | Var'(var) -> (match var with
					       | VarParam'(var, _) 
					       | VarBound'(var, _, _) -> if (var = p) then true
					                                else (isSet p value)
					       | _ -> false)
			      | _ -> (isSet p value))
       | Def'(_, value) -> (isSet p value)
       | Or'(lst) ->  (check_lst p lst isSet)
       | LambdaSimple'(params, body1) -> if (List.mem p params) then false
		                         else (isSet p body1)
       | LambdaOpt'(params, opt, body1) -> let params1 = List.append params [opt] in
	                                   if (List.mem p params1) then false
		                           else (isSet p body1)
       | Applic'(op, args) -> (isSet p op) || (check_lst p args isSet)
       | ApplicTP'(op, args) -> (isSet p op) || (check_lst p args isSet))

   and isGet p body = 
      (match body with
       | Const'(_) | Box'(_) | BoxGet'(_) | BoxSet'(_) -> false
       | Var'(var) -> (match var with
		       | VarParam'(x, _) 
		       | VarBound'(x, _, _) -> if (x = p) then true
		                               else false
		       | _ -> false)
       | If'(pred, consq, alt) -> (isGet p pred) ||
		                  (isGet p consq) ||
		                  (isGet p alt)
       | Seq'(lst) -> (check_lst p lst isGet)
       | Set'(_, value) -> (isGet p value)
       | Def'(_, value) -> (isGet p value)
       | Or'(lst) ->  (check_lst p lst isGet)
       | LambdaSimple'(params, body1) -> if (List.mem p params) then false
		                         else (isGet p body1)
       | LambdaOpt'(params, opt, body1) -> let params1 = List.append params [opt] in
	                                   if (List.mem p params1) then false
		                           else (isGet p body1)
       | Applic'(op, args) -> (isGet p op) || (check_lst p args isGet)
       | ApplicTP'(op, args) -> (isGet p op) || (check_lst p args isGet))

;;
 
let box_set e = (box_set_rec e []);;

let run_semantics expr =
 box_set 
    (annotate_tail_calls
       (annotate_lexical_addresses expr));;
  
end;; (* struct Semantics *)

module type CODE_GEN = sig
  val code_gen : expr'->((int*string*int) list)->((int*string) list)->((sexpr*int*string) list) -> string
  val compile_scheme_file : string -> string -> unit
end;;

module Code_Gen : CODE_GEN = struct

let built_in_lst = ["append" ; "apply" ; "<" ; "=" ; ">" ; "+" ; "/" ; "*" ; "-" ; "boolean?" ; "car" ; "cdr" ; "char->integer" ; "char?" ;  "cons" ; "denominator" ; "eq?" ; "integer?" ; "integer->char" ; "list"; "make-string" ; "make-vector" ; "map" ; "not" ; "null?" ; "number?" ; "numerator" ; "pair?" ; "procedure?" ; "rational?" ; "remainder" ; "set-car!" ; "set-cdr!" ; "string-length" ; "string-ref" ; "string-set!" ; "string->symbol" ; "string?" ; "symbol?" ; "symbol->string" ; "vector" ; "vector-length" ; "vector-ref" ; "vector-set!" ; "vector?" ; "zero?" ; "gcd"; "display"] 
;;

let make_make_label name =
  let counter = ref 0
  in
  fun () ->
  ( counter := !counter + 1;
    Printf.sprintf "%s_%d" name (!counter))
;;

let make_if_labels =
  let make_if_else = make_make_label "L_if_else" in
  let make_if_end = make_make_label "L_if_end" in
  fun () ->
  (make_if_else(), make_if_end())
;;

let make_lambda_labels =
  let make_lambda_code = make_make_label "L_clos_code" in
  let make_lambda_exit = make_make_label "L_clos_exit" in
  fun () ->
  (make_lambda_code(), make_lambda_exit())
;;

let make_loop_labels =
  let make_loop = make_make_label "L_loop" in
  let make_loop_end = make_make_label "L_loop_end" in
  fun () ->
  (make_loop(), make_loop_end())
;;

let make_or_label =
  let make_or_end = make_make_label "L_or_end" in
  fun () -> make_or_end()
;;

let make_applic_label =
  let make_applic = make_make_label "L_not_proc" in
  fun () -> make_applic()
;;

let rec get_addr x const_table =
  match const_table with
   | [] -> 911
   | car :: cdr ->
      (match car with
       | (sexpr , label, _) ->
	  if (x = sexpr) then label
	  else (get_addr x cdr))
;;

let rec get_addr_symbol x symbol_table =
  match symbol_table with
   | [] -> 911
   | car :: cdr ->
      (match car with
       | (label , str, _) ->
	  if (x = str) then label
	  else (get_addr_symbol x cdr))
;;

let rec get_addr_var x var_table =
  match var_table with
   | [] -> 911
   | car :: cdr ->
      (match car with
       | (label , str) ->
	  if (x = str) then label
	  else (get_addr_var x cdr))
;;

let code_gen e symbol_table var_table const_table = 
  let rec code_gen_rec e env =
    let f_addr = (get_addr (Bool false) const_table) in
    let void_addr = (get_addr Void const_table) in
  (match e with 
   | Const'(c) -> (gen_const c)
   | Or'(lst) -> let end_or = make_or_label() in
		 (gen_or lst end_or f_addr env) ^ end_or ^ ": \n"
   | If'(pred, consq, alt) -> (gen_if pred consq alt f_addr env)
   | Def'(Var'(VarFree'(name)), value) -> (gen_def name value void_addr env)
   | Set'(Var'(var), value) -> (gen_set var value void_addr env)    
   | Seq'(lst) -> (gen_seq lst env) (*^ "POP(R0);\n"*)
   | Var'(var) -> (gen_var var var_table) 
   | LambdaSimple'(params, body) ->
      let (clos_code, clos_end) = make_lambda_labels() in
      let (loop_start1, loop_end1) = make_loop_labels() in
      let (loop_start2, loop_end2) = make_loop_labels() in
       (gen_closure params env clos_code clos_end loop_start1
		       loop_end1 loop_start2 loop_end2) ^
		    (gen_simple body params env clos_code clos_end)
   | LambdaOpt'(params, opt, body) -> 
      let (clos_code, clos_end) = make_lambda_labels() in
      let (loop_start1, loop_end1) = make_loop_labels() in
      let (loop_start2, loop_end2) = make_loop_labels() in
      let (loop_start3, loop_end3) = make_loop_labels() in
      let (loop_start4, loop_end4) = make_loop_labels() in
(gen_closure params env clos_code clos_end loop_start1 loop_end1 loop_start2 loop_end2) ^ (gen_opt body params opt env clos_code clos_end loop_start3 loop_end3 loop_start4 loop_end4)
   | Applic'(op, args) -> (gen_applic op args env)
   | ApplicTP'(op, args) ->  let (loop_start, loop_end) = make_loop_labels() in
		   (gen_applic_tp op args loop_start loop_end env)
   | Box'(var) -> (gen_box var void_addr)
   | BoxGet'(var) -> (gen_box_get var)
   | BoxSet'(var, exp) -> (gen_box_set var exp void_addr env)


   | _ -> raise X_this_should_not_happen)


and overrun_frame loop_start loop_end =
"  MOV(R13, IMM(0));\n " ^ loop_start ^ ":
   CMP(R13, R15);
   JUMP_EQ(" ^ loop_end ^ ");
   MOV(STACK(R9+R13), STACK(R14+R13));
   INCR(R13);
   JUMP(" ^ loop_start ^ ");\n" ^
   loop_end ^ ":\n"

and gen_applic_tp op args loop_start loop_end env =
  let m = List.length args in
  let rev_args = List.rev args in
  let str = (args_str rev_args env) in 
   str ^
"  PUSH(" ^ (string_of_int m) ^ ");\n" ^
  (code_gen_rec op env) ^
"  CMP(INDD(R0, 0), T_CLOSURE);
   JUMP_NE(L_not_proc);
   PUSH(INDD(R0, 1));
   PUSH(FPARG(-1)); //return
   MOV(R15, SP);
   MOV(R14, FP);
   SUB(R15, R14); //sp-fp
   MOV(R8, FPARG(1)); //R8-num of args
   MOV(FP, FPARG(-2)); //old fp
   MOV(R9, R14);
   SUB(R9, IMM(4));
   SUB(R9, R8);" ^
  (overrun_frame loop_start loop_end) ^ 
"  DROP(4+R8);
   JUMPA(INDD(R0, 2));\n"

and gen_opt body params opt env clos_code clos_end loop_start3 loop_end3 loop_start4 loop_end4 =
  let params_ln =  List.length params in
  let params_ln = (params_ln + 1) in
   clos_code ^ ":\nPUSH(FP);
   MOV(FP, SP);\n" ^
   (stack_correct params_ln opt loop_start3 loop_end3 loop_start4 loop_end4) ^
"  CMP(FPARG(1), IMM(" ^ (string_of_int params_ln) ^ "));
   JUMP_NE(L_invalid_num_args);" ^
   (code_gen_rec body (List.append env params)) ^
  "POP(FP);
   RETURN;\n" ^
   clos_end ^ ":\n"

and stack_correct params_ln opt loop_start3 loop_end3 loop_start4 loop_end4 = 
"  MOV(R11, FPARG(-1));
   MOV(R13, FPARG(0));
   MOV(R8, FPARG(1)); 
   MOV(R7, " ^ (string_of_int params_ln) ^ "); 
   MOV(R6, IMM(1000)); 
   INCR(R8);\n" ^ 
loop_start3  ^ ":
   CMP(R8, R7);
   JUMP_EQ(" ^ loop_end3 ^ ");
   PUSH(R6);
   PUSH(FPARG(R8));
   CALL(MAKE_SOB_PAIR);
   MOV(R6, R0);
   DECR(R8);
   DROP(IMM(2));
   JUMP(" ^ loop_start3 ^ ");\n" ^ loop_end3 ^ ":
   MOV(SP, R14); 
   PUSH(R6);
   MOV(R8, R7);\n" 
^ loop_start4 ^ ":
   CMP(R8, IMM(1));
   JUMP_EQ(" ^ loop_end4 ^ ");
   PUSH(FPARG(R8));
   DECR(R8);
   JUMP(" ^ loop_start4 ^ ");\n" ^ loop_end4 ^ ":
   PUSH(R7);
   PUSH(R13);
   PUSH(R11);
   PUSH(SP);
   MOV(FP,SP);\n" 

and gen_box var void_addr =
  (match var with
   | VarParam'(name, minor) -> 
"  PUSH(IMM(1));
   CALL(MALLOC);
   DROP(1);
   MOV(FPARG(2+"^ (string_of_int minor) ^ "), R0);\n"
   | VarBound'(name, major, minor) -> 
"  PUSH(IMM(1));
   CALL(MALLOC);
   DROP(1);
   MOV(R1, FPARG(0));
   MOV(R1, INDD(R1, " ^ (string_of_int major) ^ "));
   MOV(INDD(R1, " ^ (string_of_int minor) ^ "), R0);;\n"
   | VarFree'(name) -> let value = (get_addr_var name var_table) in
"  PUSH(IMM(1));
   CALL(MALLOC);
   DROP(1);
   MOV(IND(" ^ (string_of_int value) ^ "), R0);" ^
"  MOV(R0, IND(" ^ (string_of_int value) ^ ");\n")

and gen_box_get var =
  (match var with
   | VarParam'(name, minor) -> 
"  MOV(R0, FPARG(2+" ^ (string_of_int minor) ^ "));
   MOV(R0, IND(R0));\n"
   | VarBound'(name, major, minor) ->
"  MOV(R0, FPARG(0));
   MOV(R0, INDD(R0, " ^ (string_of_int major) ^ "));
   MOV(R0, INDD(R0, " ^ (string_of_int minor) ^ "));
   MOV(R0, IND(R0));\n"
   | VarFree'(name) -> let value = (get_addr_var name var_table) in
"  MOV(R0, IND(" ^ (string_of_int value) ^ "));
   MOV(R0, IND(R0));\n")

and gen_box_set var exp void_addr env =
  let exp_gen = (code_gen_rec exp env) in
  (match var with
   | VarParam'(name, minor) -> exp_gen ^
"  MOV(R1, FPARG(2+" ^ (string_of_int minor) ^ "));
   MOV(IND(R1), R0);\n
   MOV(R0, " ^ (string_of_int void_addr) ^ ");\n"
   | VarBound'(name, major, minor) -> exp_gen ^
"  MOV(R1, FPARG(0));
   MOV(R1, INDD(R1, " ^ (string_of_int major) ^ "));
   MOV(R1, INDD(R1, " ^ (string_of_int minor) ^ "));
   MOV(IND(R1), R0);\n
   MOV(R0, " ^ (string_of_int void_addr) ^ ");\n"
   | VarFree'(name) -> let value = (get_addr_var name var_table) in
		       exp_gen ^
"  MOV(R1, IND(" ^ (string_of_int value) ^ "));
   MOV(IND(R1), R0);\n
   MOV(R0, " ^ (string_of_int void_addr) ^ ");\n")

and gen_applic op args env =
  let m = List.length args in
  let rev_args = List.rev args in
  let str = (args_str rev_args env) in 
  str ^
 "PUSH(" ^ (string_of_int m) ^ ");\n" ^
  (code_gen_rec op env) ^
"  CMP(INDD(R0, 0), T_CLOSURE);
   JUMP_NE(L_not_proc);
   PUSH(INDD(R0, 1));
   CALLA(INDD(R0, 2));
   DROP(2+STARG(0));\n"

and args_str lst env =
  (match lst with
   | [] -> ""
   | car :: cdr ->
      (code_gen_rec car env) ^ "PUSH(R0);\n" ^  (args_str cdr env))

and gen_closure params env clos_code clos_end loop_start1 loop_end1 loop_start2 loop_end2 =
  let env_size = List.length env in 
  let new_env = (env_size + 1) in
"  PUSH(IMM(" ^ (string_of_int new_env) ^ "));
   CALL(MALLOC);
   DROP(1);
   MOV(R1, R0);
   MOV(R2, FPARG(0));\n" ^

   (extend_env env_size loop_start1 loop_end1) ^
"  PUSH(IMM(FPARG(1)));
   CALL(MALLOC);
   DROP(1);
   MOV(R3, R0);\n" ^
   (copy_args loop_start2 loop_end2) ^ 
"  MOV(IND(R1), R3);
   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), R1);
   MOV(INDD(R0, 2), LABEL(" ^ clos_code ^ "));
   JUMP(" ^ clos_end ^ ");\n"

and extend_env env_size loop_start loop_end = 
"  MOV(R3, IMM(0));
   MOV(R4, IMM(1));\n" ^
   loop_start ^ ":\n" ^
"  CMP(R3, " ^ (string_of_int env_size) ^ ");
   JUMP_GE(" ^ loop_end ^ ");
   MOV(INDD(R1, R4), INDD(R2, R3));
   INCR(R3);
   INCR(R4);
   JUMP(" ^ loop_start ^ ");\n" ^
   loop_end ^ ":\n"

and copy_args loop_start loop_end =
"  MOV(R4, IMM(0));\n" ^
   loop_start ^ ":\n" ^
"  CMP(R4, FPARG(1));
   JUMP_GE(" ^ loop_end ^ ");
   MOV(INDD(R3, R4), FPARG(2+R4));
   INCR(R4);
   JUMP(" ^ loop_start ^ ");\n" ^
   loop_end ^ ":\n" 
   
and gen_simple body params env clos_code clos_end = 
  let params_ln =  List.length params in
   clos_code ^ ":\nPUSH(FP);
   MOV(FP, SP);\n" ^
"  CMP(FPARG(1), IMM(" ^ (string_of_int params_ln) ^ "));
   JUMP_NE(L_invalid_num_args);\n" ^
   (code_gen_rec body (List.append env params)) ^
  "POP(FP);
   RETURN;\n" ^
   clos_end ^ ":\n"

and gen_var var var_table =
  (match var with
	 | VarParam'(name, minor) -> 
"  MOV(R0, FPARG(2+" ^ (string_of_int minor) ^ "));\n"
	 | VarBound'(name, major, minor) ->
"  MOV(R0, FPARG(0));
   MOV(R0, INDD(R0, " ^ (string_of_int major) ^ "));
   MOV(R0, INDD(R0, " ^ (string_of_int minor) ^ "));\n"
	 | VarFree'(name) -> let value = (get_addr_var name var_table) in 
"  MOV(R0, IND(" ^ (string_of_int value) ^ "));\n")

and gen_seq lst env =
  (match lst with
   | [] -> ""
   | car :: cdr -> (code_gen_rec car env)
		   (*^ "PUSH(R0);\nCALL(WRITE_SOB);\n"*)
                   ^ (gen_seq cdr env))

and gen_set var value void_addr env =
  let value = (code_gen_rec value env) in
  
(match var with
   | VarParam'(name, minor) -> value ^ "MOV(FPARG(" ^ (string_of_int minor) ^ "+2), R0);\n"
			       ^ "MOV(R0, " ^ (string_of_int void_addr) ^ ");\n"
   | VarBound'(name, major, minor) -> value ^  "MOV(R1, FPARG(0));\nMOV(R1, INDD(R1, " ^ 
				      (string_of_int major) ^ "));\n" ^
				     "MOV(INDD(R1, " ^ (string_of_int minor) ^ "), R0);\n"
				     ^ "MOV(R0, " ^ (string_of_int void_addr) ^ ");\n"
   | VarFree'(name) -> let free = (get_addr_var name var_table) in
		       (* add undefined for free var *) value ^
		       "MOV(IND(" ^ (string_of_int free) ^ "), R0);\n"
                       ^ "MOV(R0, " ^ (string_of_int void_addr) ^ ");\n")

and gen_def name value void_addr env =
  let value = (code_gen_rec value env) in
  let name_addr = (get_addr_var name var_table) in
  value ^ "MOV(IND(" ^ (string_of_int name_addr) ^ "), R0);\n"
  ^ "MOV(R0, " ^ (string_of_int void_addr) ^ ");\n"	  

and gen_if pred consq alt f_addr env =
  let (else_label, end_if) = make_if_labels() in
  (code_gen_rec pred env) ^ "\n" ^ "CMP(R0, " ^ (string_of_int f_addr) ^
    ");\n" ^ "JUMP_EQ(" ^ else_label ^ ");\n"
  ^ (code_gen_rec consq env) ^ "\n" ^ "JUMP(" ^ end_if ^ ");\n" ^ else_label ^": \n"
  ^ (code_gen_rec alt env) ^ "\n" ^ end_if ^ ": "
  
and gen_const c =
  (match c with
      | Nil | Void | Bool(_) | Number(_) | Char(_) | String(_) -> 
			      let addr = (get_addr c const_table) in
			      "MOV(R0, IMM(" ^ (string_of_int addr) ^ "));\n"
      | Symbol(x) -> let addr = (get_addr_symbol x symbol_table) in
                    "MOV(R0, IMM(" ^ (string_of_int addr) ^ "));\n"
      | Pair(x,y) -> let addr = (get_addr c const_table) in 
		    "MOV(R0, IMM(" ^ (string_of_int addr) ^ "));\n"
      | Vector(lst) -> let addr = (get_addr c const_table) in 
		    "MOV(R0, IMM(" ^ (string_of_int addr) ^ "));\n")

and gen_or lst end_or f_addr env =
  (match lst with
   | last :: [] -> (code_gen_rec last env)
   | car :: cdr -> 
      (code_gen_rec car env) ^
      "CMP(R0, " ^ (string_of_int f_addr) ^ ");\n" ^ "JUMP_NE(" ^ end_or ^ ");\n" ^ 
      (gen_or cdr end_or f_addr env)
   | _ -> raise X_this_should_not_happen)


in (code_gen_rec e [])
;;

let file_to_string input_file =
  let in_channel = open_in input_file in
  let rec run () =
    try 
      let ch = input_char in_channel in ch :: (run ())
    with End_of_file ->
      ( close_in in_channel;
        [] )
  in list_to_string (run ());;

let string_to_file output_file out_string =
  let out_channel = open_out output_file in
  ( output_string out_channel out_string;
    close_out out_channel );;

let rec code_gen_lst lst symbol_table var_table const_table =
  (match lst with
   | [] -> ""
   | car::cdr -> (code_gen car symbol_table var_table const_table) ^
                        (code_gen_lst cdr symbol_table var_table const_table));;

let compile_scheme_file scm_source_file asm_target_file = 
  raise X_not_yet_implemented;;

let rec make_three_lst const_table label =
  match const_table with
   | [] -> []
   | car :: cdr ->
      (match car with
       | Void -> List.append [(car, label, "T_VOID")]
			      (make_three_lst cdr (label+1))
       | Bool(_) -> List.append [(car, label, "T_BOOL")]
	                      (make_three_lst cdr (label+2))
       | Nil -> List.append [(car, label, "T_NIL")]
			      (make_three_lst cdr (label+1))
       | Number(Int(_)) -> List.append [(car, label, "T_INTEGER")]
			      (make_three_lst cdr (label+2))
       | Number(Fraction(_)) -> List.append [(car, label, "T_FRACTION")]
			      (make_three_lst cdr (label+3))
       | Char(_) -> List.append [(car, label, "T_CHAR")]
			      (make_three_lst cdr (label+2))
       | String(x) -> List.append [(car, label, "T_STRING")]
			      (make_three_lst cdr (label+(String.length x)+2))
       | Symbol(x) -> List.append [(car, label, "T_SYMBOL")]
		              (make_three_lst cdr (label+2))
       | Pair(_, _) -> List.append [(car, label, "T_PAIR")]
			      (make_three_lst cdr (label+3))
       | Vector(x) -> List.append [(car, label, "T_VECTOR")]    
	                      (make_three_lst cdr (label+(List.length x)+2)))
;;

let rec count_const const_table =
  let last = List.hd (List.rev const_table) in
  match last with
  | (sexpr, label, _) ->
     (match sexpr with
        | Void -> (label+1)
        | Bool(_) ->  (label+2)
	| Nil ->  (label+1)
	| Number(Int(_)) ->  (label+2)
	| Number(Fraction(_)) ->  (label+2)
	| Char(_) ->  (label+2)
        | String(x) -> (label+(String.length x)+2)
        | Symbol(_) ->  (label+2)
        | Pair(_, _) ->  (label+3)
        | Vector(x) -> (label+(List.length x)+2))
;;

let rec find_const expr =
  (match expr with
  | Const'(x) -> (sexpr_const x)
  | Var'(_) | Box'(_) | BoxGet'(_) -> []
  | BoxSet'(_, value) -> (find_const value)
  | If'(pred, consq, alt) -> (List.append (find_const pred) 
                             (List.append (find_const consq) (find_const alt)))
  | Seq'(lst) -> List.flatten (List.map find_const lst) 
  | Set'(_, value) -> (find_const value)
  | Def'(_, value) -> (find_const value)
  | Or'(lst) -> List.flatten (List.map find_const lst) 
  | LambdaSimple'(params, body) -> (find_const body)
  | LambdaOpt'(params, opt, body) -> (find_const body)
  | Applic'(op, args) -> (List.append (find_const op) 
                                      (List.flatten (List.map find_const args)))
  | ApplicTP'(op, args) -> (List.append (find_const op) 
                                      (List.flatten (List.map find_const args))))

  and sexpr_const sexpr =
  (match sexpr with
     | Nil | Void | Bool(_) -> []
     | Number(_) | Char(_) | String(_) -> [sexpr]
     | Symbol(x) -> List.append [sexpr] [String(x)]
     | Pair(x, y) -> (List.append (List.append (sexpr_const x) 
					       (sexpr_const y)) [sexpr])
     | Vector(sexpr_lst) -> List.append (List.flatten
					(List.map sexpr_const sexpr_lst)) [sexpr])
;;

let make_cisc_const_wrap const_table dynamic_table =
  let rec make_cisc_const dynamic_table =
    match dynamic_table with
   | [] -> ""
   | car :: cdr ->
      (match car with
       | (sexpr,label,scm_type) ->
	  (match sexpr with
	   | Void | Nil -> "MOV(ADDR(" ^ (string_of_int label) ^ "), IMM(" 
	             ^ scm_type ^ "))" ^ ";\n" ^ (make_cisc_const cdr)
	   | Bool(x) -> "MOV(ADDR(" ^ (string_of_int label) ^ "), IMM(" 
	             ^ scm_type ^ "))" ^ ";\n"^ "MOV(ADDR(" ^ 
		       (string_of_int (label+1)) ^ 
		     "), IMM(" ^ (string_of_int (get_bool x)) 
		     ^ "))" ^ ";\n" ^ (make_cisc_const cdr)
           | Number(Int(x)) -> "MOV(ADDR(" ^ (string_of_int label) ^ "), IMM(" 
	             ^ scm_type ^ "))" ^ ";\n" ^ "MOV(ADDR(" ^ 
		       (string_of_int (label+1)) ^ 
		     "), IMM(" ^ (string_of_int x) ^ "))" ^ ";\n" ^ 
		       (make_cisc_const cdr)
           | Number(Fraction(x)) -> "MOV(ADDR(" ^ (string_of_int label) ^ "), IMM(" 
	             ^ scm_type ^ "))" ^ ";\n" ^ "MOV(ADDR(" ^ 
		       (string_of_int (label+1)) ^ 
		     "), IMM(" ^ (string_of_int x.numerator) ^ 
		       "));\nMOV(ADDR(" ^ (string_of_int (label+2)) ^ 
		     "), IMM(" ^ (string_of_int x.denominator) ^ "))" 
			      ^ ";\n" ^ (make_cisc_const cdr)
           | Char(x) -> "MOV(ADDR(" ^ (string_of_int label) ^ "), IMM(" 
	             ^ scm_type ^ "))" ^ ";\n" ^ "MOV(ADDR(" ^ 
		       (string_of_int (label+1)) ^ 
		     "), IMM(" ^ (string_of_int (int_of_char x)) ^ "))" ^ ";\n" ^ 
		      (make_cisc_const cdr)
	   | String(x)  -> 
	         "MOV(ADDR(" ^ (string_of_int label) ^
	             "), IMM(" ^ scm_type ^ "))" ^ ";\n" ^ 
                 "MOV(ADDR(" ^ (string_of_int (label+1)) ^
	            "), IMM(" ^ (string_of_int (String.length x)) ^ "))" ^ ";\n" ^
		      (print_string (string_to_list x) (label+2)) 
				      ^ (make_cisc_const cdr)
           | Symbol(x) -> let x_addr = (get_addr (String(x)) const_table) in 
	      "MOV(ADDR(" ^ (string_of_int label) ^
	             "), IMM(" ^ scm_type ^ "))" ^ ";\n" ^ 
                       "MOV(ADDR(" ^ (string_of_int (label+1)) ^
			 "), IMM(" ^ (string_of_int x_addr) ^ "));\n"  
				      ^ (make_cisc_const cdr) 
	   | Pair(x, y) ->
	      "MOV(ADDR(" ^ (string_of_int label) ^ 
		        "), IMM(" ^ scm_type ^ "))" ^ ";\n" ^
	      "MOV(ADDR(" ^ (string_of_int (label+1)) ^ 
		        "), IMM(" ^ (string_of_int (get_addr x const_table)) ^ "))" ^ ";\n" ^
              "MOV(ADDR(" ^ (string_of_int (label+2)) ^ 
		        "), IMM(" ^ (string_of_int (get_addr y const_table)) ^ "))" ^ ";\n"
			^ (make_cisc_const cdr)
           | Vector(lst) -> 
	      "MOV(ADDR(" ^ (string_of_int label) ^
		"), IMM(" ^ scm_type ^ "))" ^ ";\n" ^
	       "MOV(ADDR(" ^ (string_of_int (label+1)) ^
		 "), IMM(" ^ (string_of_int (List.length lst)) ^ "))" ^ ";\n" ^
		   (get_vector_lst lst (label+2) const_table)
		          ^ (make_cisc_const cdr)))

and get_vector_lst lst label const_table =
  let rec get_vector_lst_rec lst label =
    (match lst with
    | [] -> ""
    | car::cdr -> "MOV(ADDR(" ^ (string_of_int label) ^ 
		   "), IMM(" ^ (string_of_int (get_addr car const_table)) ^ "))"
		   ^ ";\n" ^ (get_vector_lst_rec cdr (label+1)))
in (get_vector_lst_rec lst label)

and print_string string_lst label =
  (match string_lst with 
   | [] -> ""
   | car :: cdr ->
      "MOV(ADDR(" ^ (string_of_int (label)) ^ "), IMM('" ^ (list_to_string [car]) 
      ^ "'))" ^ ";\n" ^ (print_string cdr (label+1)))
  
and get_bool bool =
  match bool with
  | true -> 1
  | false -> 0

in (make_cisc_const dynamic_table)
;;

let make_const_table expr_file_lst =
  List.flatten (List.map find_const expr_file_lst)
;;

let rec remove_double const_table =
  (match const_table with
  | [] -> []
  | car :: cdr ->
     if (List.mem car cdr) then 
       (remove_double cdr)
     else List.append [car] (remove_double cdr))
;;



let rec make_global_variable expr_lst start =
  let lstOfFreeVars = (make_freeVars_table expr_lst) in
  let lstOfFreeVars = List.append built_in_lst lstOfFreeVars in
  let lstOfFreeVars = (remove_double lstOfFreeVars) in 
  let lstOfFreeVars = (make_doubles lstOfFreeVars start) in 
  lstOfFreeVars 

and make_doubles lst label = 
  (match lst with
   | [] -> []
   | car :: cdr -> (List.append [(label, car)]
		   (make_doubles cdr (label+1))))

and find_free_vars expr =
  (match expr with
   | Const'(x) -> []
   | Var'(var) -> (match var with
		   | VarFree'(x) -> [x]
		   | _ -> [] )
  | Box'(_) | BoxGet'(_) -> []
  | BoxSet'(_, value) -> (find_free_vars value)
  | If'(pred, consq, alt) -> (List.append (find_free_vars pred) 
                             (List.append (find_free_vars consq) (find_free_vars alt)))
  | Seq'(lst) -> List.flatten (List.map find_free_vars lst) 
  | Set'(var, value) -> (find_free_vars var)
  | Def'(var, value) -> (find_free_vars var)
  | Or'(lst) -> List.flatten (List.map find_free_vars lst) 
  | LambdaSimple'(params, body) -> (find_free_vars body)
  | LambdaOpt'(params, opt, body) -> (find_free_vars body)
  | Applic'(op, args) -> (List.append (find_free_vars op) 
                                      (List.flatten (List.map find_free_vars args)))
  | ApplicTP'(op, args) -> (List.append (find_free_vars op) 
                                      (List.flatten (List.map find_free_vars args))))
  
and make_freeVars_table expr_lst =
  (match expr_lst with
   | [] -> []
   | car :: cdr ->
      List.append (find_free_vars car) 
		  (make_freeVars_table cdr))
;;

let rec make_symbol_table const_table const_table1 =
  match const_table with   
   | [] -> []
   | car :: cdr ->
      (match car with
       | (sexpr,label,scm_type) ->
	  (match sexpr with
	   | (Symbol(x)) -> let s_addr = (get_addr (String(x)) const_table1) in
	      List.append [(label, x, s_addr)]
				       (make_symbol_table cdr const_table1)
           | _ -> (make_symbol_table cdr const_table1)))
;;
     
let rec make_cisc_symbol symbol_table =
  match symbol_table with
   | [] -> ""
   | car::cdr ->
      (match car with 
       | (label, _, _) -> 
"  PUSH(IMM(2));
   CALL(MALLOC);
   DROP(1);
   MOV(IND(R0),IMM(" ^ (string_of_int label) ^ "));
   MOV(INDD(R0,1),R1);
   MOV(R1,R0);\n"
   ^ (make_cisc_symbol cdr))
;;

let make_cisc_symbol_nil const_table symbol_table =
   let nil = (get_addr Nil const_table) in
   let str = 
"   MOV(R1, IMM(" ^ (string_of_int nil) ^ "));\n" in
   str ^ (make_cisc_symbol symbol_table) ^
"   MOV(R12, R1); //R12 - symbol table (need to go backwards)" ^ "\n"
;;

let count_var var_table =
  let last = List.hd (List.rev var_table) in
  match last with
  | (label, name) -> (label+1)
;; 

let prologue var_table = 
  let end_of_var = (count_var var_table) in
  let cons_addr = (get_addr_var "cons" var_table) in
  let pair_addr = (get_addr_var "pair?" var_table) in
  let car_addr = (get_addr_var "car" var_table) in
  let cdr_addr = (get_addr_var "cdr" var_table) in
  let number_addr = (get_addr_var "number?" var_table) in
  let nil_addr = (get_addr_var "null?" var_table) in
  let plus_addr = (get_addr_var "+" var_table) in
  let mul_addr = (get_addr_var "*" var_table) in
  let div_addr = (get_addr_var "/" var_table) in
  let append_addr = (get_addr_var "append" var_table) in
  let rational_addr = (get_addr_var "rational?" var_table) in
  let sub_addr = (get_addr_var "-" var_table) in
  let integer_addr = (get_addr_var "integer?" var_table) in
  let bool_addr = (get_addr_var "boolean?" var_table) in
  let char_addr = (get_addr_var "char?" var_table) in
  let proc_addr = (get_addr_var "procedure?" var_table) in
  let string_addr = (get_addr_var "string?" var_table) in
  let symbol_addr = (get_addr_var "symbol?" var_table) in
  let vector_addr = (get_addr_var "vector?" var_table) in
  let zero_addr = (get_addr_var "zero?" var_table) in
  let greater_addr = (get_addr_var ">" var_table) in
  let lower_addr = (get_addr_var "<" var_table) in
  let equal_addr = (get_addr_var "=" var_table) in  
  let vector_init_addr = (get_addr_var "vector" var_table) in
  let make_vector_addr = (get_addr_var "make-vector" var_table) in
  let vector_len_addr = (get_addr_var "vector-length" var_table) in
  let string_len_addr = (get_addr_var "string-length" var_table) in
  let vector_ref_addr = (get_addr_var "vector-ref" var_table) in
  let string_ref_addr = (get_addr_var "string-ref" var_table) in
  let list_addr = (get_addr_var "list" var_table) in
  let set_car_addr = (get_addr_var "set-car!" var_table) in
  let set_cdr_addr = (get_addr_var "set-cdr!" var_table) in
  let integer_char_addr = (get_addr_var "integer->char" var_table) in
  let char_integer_addr = (get_addr_var "char->integer" var_table) in
  let vector_set_addr = (get_addr_var "vector-set!" var_table) in
  let string_set_addr = (get_addr_var "string-set!" var_table) in
  let make_string_addr = (get_addr_var "make-string" var_table) in
  let symbol_string_addr = (get_addr_var "symbol->string" var_table) in
  let string_symbol_addr = (get_addr_var "string->symbol" var_table) in
  let not_addr = (get_addr_var "not" var_table) in
  let numerator_addr = (get_addr_var "numerator" var_table) in
  let denominator_addr = (get_addr_var "denominator" var_table) in
  let eq_addr = (get_addr_var "eq?" var_table) in
  let remainder_addr = (get_addr_var "remainder" var_table) in
  let map_addr = (get_addr_var "map" var_table) in
  let apply_addr = (get_addr_var "apply" var_table) in
  let gcd_addr = (get_addr_var "gcd" var_table) in
  let display_addr = (get_addr_var "display" var_table) in

  "#include <stdio.h>
   #include <stdlib.h>
   #define DO_SHOW 1
   #include \"cisc.h\"
   #include \"debug.h\"

int main()
{
  START_MACHINE;

  JUMP(CONTINUE);

#include \"char.lib\"
#include \"io.lib\"
#include \"math.lib\"
#include \"string.lib\"
#include \"system.lib\"
#include \"scheme.lib\"
#include \"primitive.lib\"

   PUSH(IMM(0));
   PUSH(IMM(0));
   PUSH(LABEL(L_exit));
   PUSH(FP);
   MOV(FP, SP);
CONTINUE:
   MOV(ADDR(0), IMM(" ^ (string_of_int end_of_var) ^ "));
   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_cons));
   MOV(IND(" ^ (string_of_int cons_addr) ^ "), R0);
   
   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_pair));
   MOV(IND(" ^ (string_of_int pair_addr) ^ "), R0); 

   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_car));
   MOV(IND(" ^ (string_of_int car_addr) ^ "), R0); 
  
   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_cdr));
   MOV(IND(" ^ (string_of_int cdr_addr) ^ "), R0); 

   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_number));
   MOV(IND(" ^ (string_of_int number_addr) ^ "), R0); 

   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_nil));
   MOV(IND(" ^ (string_of_int nil_addr) ^ "), R0); 

   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_plus));
   MOV(IND(" ^ (string_of_int plus_addr) ^ "), R0);

   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_mul));
   MOV(IND(" ^ (string_of_int mul_addr) ^ "), R0);

   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_div));
   MOV(IND(" ^ (string_of_int div_addr) ^ "), R0);

   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_append));
   MOV(IND(" ^ (string_of_int append_addr) ^ "), R0);
   
   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_rational));
   MOV(IND(" ^ (string_of_int rational_addr) ^ "), R0);

   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_sub));
   MOV(IND(" ^ (string_of_int sub_addr) ^ "), R0);

   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_integer));
   MOV(IND(" ^ (string_of_int integer_addr) ^ "), R0);

   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_char));
   MOV(IND(" ^ (string_of_int char_addr) ^ "), R0);

   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_proc));
   MOV(IND(" ^ (string_of_int proc_addr) ^ "), R0);

   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_bool));
   MOV(IND(" ^ (string_of_int bool_addr) ^ "), R0);
   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_string));
   MOV(IND(" ^ (string_of_int string_addr) ^ "), R0);

   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_symbol));
   MOV(IND(" ^ (string_of_int symbol_addr) ^ "), R0);

   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_vector));
   MOV(IND(" ^ (string_of_int vector_addr) ^ "), R0);

   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_zero));
   MOV(IND(" ^ (string_of_int zero_addr) ^ "), R0);

   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_greater));
   MOV(IND(" ^ (string_of_int greater_addr) ^ "), R0);

   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_lower));
   MOV(IND(" ^ (string_of_int lower_addr) ^ "), R0);

   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_equal));
   MOV(IND(" ^ (string_of_int equal_addr) ^ "), R0);
   
   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_vector_init));
   MOV(IND(" ^ (string_of_int vector_init_addr) ^ "), R0);

   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_make_vector));
   MOV(IND(" ^ (string_of_int make_vector_addr) ^ "), R0);

   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_vector_length));
   MOV(IND(" ^ (string_of_int vector_len_addr) ^ "), R0);

   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_string_length));
   MOV(IND(" ^ (string_of_int string_len_addr) ^ "), R0);

   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_vector_ref));
   MOV(IND(" ^ (string_of_int vector_ref_addr) ^ "), R0);

   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_string_ref));
   MOV(IND(" ^ (string_of_int string_ref_addr) ^ "), R0);

   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_list));
   MOV(IND(" ^ (string_of_int list_addr) ^ "), R0);

   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_set_car));
   MOV(IND(" ^ (string_of_int set_car_addr) ^ "), R0);

   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_set_cdr));
   MOV(IND(" ^ (string_of_int set_cdr_addr) ^ "), R0);

   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_integer_char));
   MOV(IND(" ^ (string_of_int integer_char_addr) ^ "), R0);

   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_char_integer));
   MOV(IND(" ^ (string_of_int char_integer_addr) ^ "), R0);

   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_vector_set));
   MOV(IND(" ^ (string_of_int vector_set_addr) ^ "), R0);

   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_string_set));
   MOV(IND(" ^ (string_of_int string_set_addr) ^ "), R0);

   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_make_string));
   MOV(IND(" ^ (string_of_int make_string_addr) ^ "), R0);

   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_symbol_string));
   MOV(IND(" ^ (string_of_int symbol_string_addr) ^ "), R0);

   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_string_symbol));
   MOV(IND(" ^ (string_of_int string_symbol_addr) ^ "), R0);

   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_not));
   MOV(IND(" ^ (string_of_int not_addr) ^ "), R0);

   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_numerator));
   MOV(IND(" ^ (string_of_int numerator_addr) ^ "), R0);

   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_denominator));
   MOV(IND(" ^ (string_of_int denominator_addr) ^ "), R0);

   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_eq));
   MOV(IND(" ^ (string_of_int eq_addr) ^ "), R0);

   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_remainder));
   MOV(IND(" ^ (string_of_int remainder_addr) ^ "), R0);

   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_map_reg));
   MOV(IND(" ^ (string_of_int map_addr) ^ "), R0);

   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_apply));
   MOV(IND(" ^ (string_of_int apply_addr) ^ "), R0);

   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_gcd));
   MOV(IND(" ^ (string_of_int gcd_addr) ^ "), R0);

   PUSH(IMM(3));
   CALL(MALLOC);
   DROP(1);
   MOV(INDD(R0, 0), T_CLOSURE);
   MOV(INDD(R0, 1), IMM(850001));
   MOV(INDD(R0, 2), LABEL(L_display));
   MOV(IND(" ^ (string_of_int display_addr) ^ "), R0);



"

let epilogue = 

"  SHOW(\"Result is: \", R0);
   CMP(R0, IMM(1001));
   JUMP_EQ(L_exit); 
   PUSH(R0);
   CALL(WRITE_SOB);
   DROP(1);
   CALL(NEWLINE);
L_exit:
   STOP_MACHINE;
   return 0;
L_invalid_num_args:
   SHOW(\"Invalid number of args \", R0);
   HALT;
L_invalid_types:
   SHOW(\"Invalid types \", R0);
   HALT;
L_not_integer:
   SHOW(\"Argument is not an integer \", R0);
   HALT;
L_not_symbol:
   SHOW(\"Argument is not a symbol \", R0);
   HALT;
L_not_proc:
   SHOW(\"Not a procedure \", R0);
   HALT;
L_not_vector:
   SHOW(\"Argument is not a vector \", R0);
   HALT;
L_not_string:
   SHOW(\"Argument is not a string \", R0);
   HALT;
L_not_char:
   SHOW(\"Argument is not a char \", R0);
   HALT;
L_not_fraction:
   SHOW(\"Argument is not a fraction \", R0);
   HALT;
L_not_list:
   SHOW(\"Argument is not a list \", R0);
   HALT;
L_var_not_defined:
   SHOW(\"Var not defined \", R0);
   HALT;
L_unbound_index:
   SHOW(\"Unbound index \", R0);
   HALT;
L_divide_zero:
   SHOW(\"Can't divide by zero \", R0);
   HALT;
}"

let compile_scheme_file scm_source_file scm_target_file =
  let stringFile = (file_to_string scm_source_file) in
  let exprFile = (List.map Semantics.run_semantics 
		    (Tag_Parser.read_expressions stringFile)) in
  let const_table = (make_const_table exprFile) in 
  let pre_const_table = [Nil; Void; Bool(true); Bool(false)] in
  let const_table = List.append pre_const_table 
				(remove_double const_table) in
(*  let offset = (count_const const_table 0) in*)
  let const_table = (make_three_lst const_table 1000) in
  let end_of_const = (count_const const_table) in
  let var_table = (make_global_variable exprFile end_of_const) in
  let symbol_table = (make_symbol_table const_table const_table) in
  let str = (code_gen_lst exprFile symbol_table var_table const_table) in
  (*et size = (string_of_int offset) in
    let const_table_alloc = 
"  PUSH(" ^ size ^ ");
   CALL(MALLOC);
   DROP(" ^ size ^ ");\n\n" in*)
  let const_table_str = (make_cisc_const_wrap const_table const_table) in
  let const_comm = "//const table\n" in
  let const_str = const_comm  ^ const_table_str  ^ "\n" in
  let symbol_table_str = (make_cisc_symbol_nil const_table symbol_table) in
  let symbol_comm = "//symbol table\n" in
  let symbol_str = symbol_comm ^ symbol_table_str  ^ "\n" in
  let prologue = (prologue var_table) in
  let str = prologue ^ const_str ^ symbol_str ^ str ^ epilogue in
  (string_to_file scm_target_file str)
;;

end;;

let test_parser string =
  let expr = Tag_Parser.read_expression string in
  let string' = (Tag_Parser.expression_to_string expr) in
  Printf.printf "%s\n" string';;

(*  append (variadic), apply, < (variadic), = (variadic), > (variadic), + (variadic), / (variadic), * (variadic), - (variadic), boolean?, car, cdr, char->integer, char?, cons, denominator, eq?, integer?, integer->char, list (variadic), make-string, make-vector, map, not, null?, number?, numerator, pair?, procedure?, rational?, remainder, set-car!, set-cdr!, string-length, string-ref, string-set!, string->symbol, string?, symbol?, symbol->string, vector, vector-length, vector-ref, vector-set!, vector?, zero?.   *)
