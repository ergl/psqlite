use "package:../psqlite"

actor Main
  new create(env: Env) =>
    try
      let db = Sqlite.open("/tmp/movies.db", SqliteOpenOptions.readwrite() or SqliteOpenOptions.createdb())?
      env.out.print("Hey!")
      let stmt = db.prepare("CREATE TABLE IF NOT EXISTS movies (title TEXT PRIMARY KEY, year INTEGER)")?
      env.out.print("Statement will generate " + stmt.columns().string() + " columns")
      stmt.execute()?
      env.out.print("Stmt done")
      db.close_stmt(consume stmt)
      // let stmt2 = db.prepare("INSERT INTO movies VALUES(\"Masculin/Feminim\", 1966)")?
      // env.out.print("Statement will generate " + stmt2.columns().string() + " columns")
      // stmt2.execute()?
      // env.out.print("Stmt done")
      // db.close_stmt(consume stmt2)
      let stmt3 = db.prepare("SELECT * FROM movies")?
      env.out.print("Statement will generate " + stmt3.columns().string() + " columns")
      if stmt3.next()? then
        let title = stmt3.string(0)
        let year = stmt3.int(1)
        env.out.print("Got " + (consume title) + "@" + year.string())
      end
      // stmt2.execute()?
      // env.out.print("Stmt done")
      db.close_stmt(consume stmt3)
      Sqlite.close(consume db)
    else
      env.out.print("oops")
    end
