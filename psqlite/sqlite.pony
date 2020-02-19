use "lib:sqlite3"
use "debug"

use @sqlite3_open_v2[U32](filename: Pointer[U8] tag,
                          handle: Pointer[Pointer[_ConnHandle] tag] tag,
                          flags: U32,
                          vfz: NullablePointer[_SqliteVFS] tag)
use @sqlite3_close_v2[U32](handle: Pointer[_ConnHandle] tag)

use @sqlite3_prepare_v3[U32](handle: Pointer[_ConnHandle] tag,
                             query: Pointer[U8] tag,
                             query_size: USize,
                             flags: U32,
                             stmt: Pointer[Pointer[_Stmt] tag] tag,
                             tail: Pointer[Pointer[U8] tag] tag)
use @sqlite3_column_count[U32](stmt: Pointer[_Stmt] tag)
use @sqlite3_step[U32](stmt: Pointer[_Stmt] tag)
use @sqlite3_finalize[U32](stmt: Pointer[_Stmt] tag)

use @sqlite3_bind_blob[U32](stmt: Pointer[_Stmt] tag, idx: USize, data: Pointer[U8]tag, size: USize, dest: Pointer[U8] tag)
use @sqlite3_bind_double[U32](stmt: Pointer[_Stmt] tag, idx: USize, double: F64)
use @sqlite3_bind_int[U32](stmt: Pointer[_Stmt] tag, idx: USize, int: I32)
use @sqlite3_bind_int64[U32](stmt: Pointer[_Stmt] tag, idx: USize, int: I64)
use @sqlite3_bind_null[U32](stmt: Pointer[_Stmt] tag, idx: USize)
use @sqlite3_bind_text[U32](stmt: Pointer[_Stmt] tag, idx: USize, data: Pointer[U8]tag, size: USize, dest: Pointer[U8] tag)

use @sqlite3_column_blob[Pointer[U8] ref](stmt: Pointer[_Stmt] tag, idx: USize)
use @sqlite3_column_double[F64](stmt: Pointer[_Stmt] tag, idx: USize)
use @sqlite3_column_int[I32](stmt: Pointer[_Stmt] tag, idx: USize)
use @sqlite3_column_int64[I64](stmt: Pointer[_Stmt] tag, idx: USize)
use @sqlite3_column_text[Pointer[U8] ref](stmt: Pointer[_Stmt] tag, idx: USize)
use @sqlite3_column_bytes[USize](stmt: Pointer[_Stmt] tag, idx: USize)
use @sqlite3_column_type[U32](stmt: Pointer[_Stmt] tag, idx: USize)

struct iso _ConnHandle
  new iso create() => None

struct iso _Stmt
  new iso create() => None

struct _SqliteVFS

class iso Connection
  let _handle: Pointer[_ConnHandle] tag

  new iso _create(handle: Pointer[_ConnHandle] tag) =>
    _handle = handle

  fun prepare(sql: String): Statement^? =>
    var stmt: Pointer[_Stmt] tag = Pointer[_Stmt].create()
    var tail: Pointer[U8] tag = Pointer[U8].create() // Will be ignored
    let ret = @sqlite3_prepare_v3(
      _handle,
      sql.cpointer(),
      sql.size(),
      0,
      addressof stmt,
      addressof tail
    )

    if ret == SqliteError.ok() then
      Statement._create(stmt)
    else
      Debug.err(" sqlite3_prepare_v3: " + ret.string())
      error
    end

  fun close_stmt(stmt: Statement iso) =>
    stmt._close()

  fun _raw_handle(): Pointer[_ConnHandle] tag =>
    _handle

primitive SqInteger
primitive SqInteger64
primitive SqFloat
primitive SqText
primitive SqBlob
primitive SqNull

type ColumnType is (SqInteger | SqInteger64 | SqFloat | SqText | SqBlob | SqNull)
class iso Statement
  let _stmt: Pointer[_Stmt] tag

  new iso _create(stmt: Pointer[_Stmt] tag) =>
    _stmt = stmt

  fun columns(): U32 =>
    @sqlite3_column_count(_stmt)

  fun bind(column: USize, value: (String | Array[U8] val | I32 | I64 | F64 | None)) =>
    var static: U8 = 0
    match value
    | let s: String => @sqlite3_bind_text(_stmt, column, s.cpointer(), s.size(), addressof static)
    | let a: Array[U8] val => @sqlite3_bind_blob(_stmt, column, a.cpointer(), a.size(), addressof static)
    | let i: I32 => @sqlite3_bind_int(_stmt, column, i)
    | let i64: I64 => @sqlite3_bind_int64(_stmt, column, i64)
    | let f: F64 => @sqlite3_bind_double(_stmt, column, f)
    | None => @sqlite3_bind_null(_stmt, column)
    end

  fun next(): Bool? =>
    let ret = @sqlite3_step(_stmt)
    match ret
    | SqliteError.done() => false
    | SqliteError.row() => true
    else error end

  fun execute()? =>
    let ret = @sqlite3_step(_stmt)
    if ret != SqliteError.done() then error end

  fun datatype(column: USize): ColumnType =>
    match @sqlite3_column_type(_stmt, column)
    | 1 => SqInteger64
    | 2 => SqFloat
    | 3 => SqText
    | 4 => SqBlob
    | 5 => SqNull
    else SqNull end

  fun double(column: USize): F64 =>
    @sqlite3_column_double(_stmt, column)

  fun int(column: USize): I64 =>
    @sqlite3_column_int64(_stmt, column)

  fun blob(column: USize): Array[U8] iso^ =>
    let size = @sqlite3_column_bytes(_stmt, column)
    recover
      let ptr = @sqlite3_column_blob(_stmt, column)
      Array[U8].from_cpointer(ptr, size)
    end

  fun string(column: USize): String iso^ =>
    let size = @sqlite3_column_bytes(_stmt, column)
    recover
      let ptr = @sqlite3_column_text(_stmt, column)
      String.copy_cpointer(ptr, size)
    end

  fun _close() =>
    @sqlite3_finalize(_stmt)

primitive Sqlite
  fun open(name: String, flags: U32): Connection iso^ ? =>
    var handle: Pointer[_ConnHandle] tag = Pointer[_ConnHandle].create()
    let ret = @sqlite3_open_v2(
      name.cpointer(),
      addressof handle,
      flags,
      NullablePointer[_SqliteVFS].none()
    )

    if ret == SqliteError.ok() then
      Connection._create(handle)
    else
      Debug.err("sqlite_open_v2: " + ret.string())
      error
    end

  fun close(conn: Connection iso) =>
    @sqlite3_close_v2(conn._raw_handle())
