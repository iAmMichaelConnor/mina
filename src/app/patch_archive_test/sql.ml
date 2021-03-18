(* sql.ml -- (Postgresql) SQL queries for patch_archive_test app *)

module Copy_database = struct
  let make_drop_db_query ~copy_db =
    Caqti_request.exec Caqti_type.unit
      (Core_kernel.sprintf {sql|DROP DATABASE %s|sql} copy_db)

  let run_drop_db (module Conn : Caqti_async.CONNECTION) ~copy_db =
    Conn.exec (make_drop_db_query ~copy_db) ()

  (* NB: SQL parameters can only be data, not identifiers *)
  let make_query ~original_db ~copy_db =
    Caqti_request.exec Caqti_type.unit
      (Core_kernel.sprintf {sql|CREATE DATABASE %s WITH TEMPLATE %s|sql}
         copy_db original_db)

  let run (module Conn : Caqti_async.CONNECTION) ~original_db ~copy_db =
    Conn.exec (make_query ~original_db ~copy_db) ()
end

module Block = struct
  let delete_query =
    Caqti_request.exec Caqti_type.string
      {sql|DELETE FROM blocks WHERE state_hash = $1|sql}

  let run_delete (module Conn : Caqti_async.CONNECTION) ~state_hash =
    Conn.exec delete_query state_hash

  let unset_parent_query =
    Caqti_request.exec Caqti_type.int
      {sql|UPDATE blocks SET parent_id = NULL WHERE parent_id = $1|sql}

  let run_unset_parent (module Conn : Caqti_async.CONNECTION) id =
    Conn.exec unset_parent_query id

  let state_hash_query =
    Caqti_request.collect Caqti_type.unit Caqti_type.string
      {sql|SELECT state_hash FROM blocks|sql}

  let run_state_hash (module Conn : Caqti_async.CONNECTION) =
    Conn.collect_list state_hash_query ()

  let query =
    Caqti_request.find Caqti_type.string Caqti_type.int
      {sql|SELECT id FROM blocks WHERE state_hash = $1|sql}

  let run (module Conn : Caqti_async.CONNECTION) ~state_hash =
    Conn.find query state_hash
end
