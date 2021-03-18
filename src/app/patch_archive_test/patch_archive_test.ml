(* patch_archive_test.ml *)

(* test patching of archive databases

   test structure:
    - copy archive database
    - remove some blocks from the copy
    - patch the copy
    - compare original and copy
*)

open Core_kernel
open Async

(* open Mina_base
open Signature_lib
   open Archive_lib *)

let db_from_uri uri =
  let path = Uri.path uri in
  String.sub path ~pos:1 ~len:(String.length path - 1)

let make_archive_copy_uri archive_uri =
  let db = db_from_uri archive_uri in
  Uri.with_path archive_uri ("/copy_of_" ^ db)

let query_db pool ~f ~item =
  match%bind Caqti_async.Pool.use f pool with
  | Ok v ->
      return v
  | Error msg ->
      failwithf "Error running query for %s from db, error: %s" item
        (Caqti_error.show msg) ()

let main ~archive_uri ~num_blocks_to_patch ~precomputed:_ ~extensional:_
    ~files:_ () =
  let logger = Logger.create () in
  let archive_uri = Uri.of_string archive_uri in
  let copy_uri = make_archive_copy_uri archive_uri in
  [%log info] "Connecting to original database" ;
  let%bind () =
    match Caqti_async.connect_pool ~max_size:128 archive_uri with
    | Error e ->
        [%log fatal]
          ~metadata:[("error", `String (Caqti_error.show e))]
          "Failed to create a Caqti pool for Postgresql" ;
        exit 1
    | Ok pool ->
        [%log info] "Successfully created Caqti pool for Postgresql" ;
        let original_db = db_from_uri archive_uri in
        let copy_db = db_from_uri copy_uri in
        [%log info] "Dropping copied database, in case it already exists" ;
        let%bind () =
          match%bind
            Caqti_async.Pool.use
              (fun db -> Sql.Copy_database.run_drop_db db ~copy_db)
              pool
          with
          | Ok () ->
              return ()
          | Error msg ->
              [%log info]
                "Dropping copied database resulted in error (probably OK): %s"
                (Caqti_error.show msg) ;
              return ()
        in
        [%log info] "Copying database" ;
        let%map () =
          query_db pool
            ~f:(fun db -> Sql.Copy_database.run db ~original_db ~copy_db)
            ~item:"database copy"
        in
        ()
  in
  [%log info] "Connecting to copied database" ;
  match Caqti_async.connect_pool ~max_size:128 copy_uri with
  | Error e ->
      [%log fatal]
        ~metadata:[("error", `String (Caqti_error.show e))]
        "Failed to create a Caqti pool for Postgresql" ;
      exit 1
  | Ok pool ->
      [%log info] "Successfully created Caqti pool for Postgresql" ;
      let%bind state_hashes =
        query_db pool
          ~f:(fun db -> Sql.Block.run_state_hash db)
          ~item:"state hashes"
      in
      let state_hash_array = Array.of_list state_hashes in
      (* indexes of block state_hashes to delete from copied database *)
      let indexes_to_delete =
        Quickcheck.random_value
          (Quickcheck.Generator.list_with_length num_blocks_to_patch
             (Int.gen_uniform_incl 0 (Array.length state_hash_array - 1)))
      in
      let%bind () =
        Deferred.List.iter indexes_to_delete ~f:(fun ndx ->
            let state_hash = state_hash_array.(ndx) in
            (* before removing block, remove any parent id references to that block
                 otherwise, we get a foreign key constraint violation
              *)
            [%log info]
              "Removing parent references to block with state hash \
               $state_hash in copied database"
              ~metadata:[("state_hash", `String state_hash)] ;
            let%bind id =
              query_db pool
                ~f:(fun db -> Sql.Block.run db ~state_hash)
                ~item:"id of block to delete"
            in
            let%bind () =
              query_db pool
                ~f:(fun db -> Sql.Block.run_unset_parent db id)
                ~item:"id of parent block to be NULLed"
            in
            [%log info]
              "Deleting block with state hash $state_hash from copied database"
              ~metadata:[("state_hash", `String state_hash)] ;
            query_db pool
              ~f:(fun db -> Sql.Block.run_delete db ~state_hash)
              ~item:"state hash of block to delete" )
      in
      Deferred.unit

let () =
  Command.(
    run
      (let open Let_syntax in
      async ~summary:"Test patching of blocks in an archive database"
        (let%map archive_uri =
           Param.flag "--archive-uri"
             ~doc:
               "URI URI for connecting to the archive database (e.g., \
                postgres://$USER:$USER@localhost:5432/archiver)"
             Param.(required string)
         and num_blocks_to_patch =
           Param.(
             flag "--num-blocks-to-patch" ~aliases:["num-blocks-to-patch"]
               Param.(required int))
             ~doc:"Number of blocks to remove and patch"
         and precomputed =
           Param.(flag "--precomputed" ~aliases:["precomputed"] no_arg)
             ~doc:"Blocks are in precomputed format"
         and extensional =
           Param.(flag "--extensional" ~aliases:["extensional"] no_arg)
             ~doc:"Blocks are in extensional format"
         and files = Param.anon Anons.(sequence ("FILES" %: Param.string)) in
         main ~archive_uri ~num_blocks_to_patch ~precomputed ~extensional
           ~files)))
