use "package:../psqlite"

primitive Schema
  fun apply(): String =>
    "
      begin transaction;

      create table users (
        usersId integer primary key,
        email text not null,
        username text,
        password text not null,
        deleted integer not null default 0
      );

      create table friends (
        usersId integer references users(usersId) on update cascade on delete cascade,
        friendId integer references users(usersId) on update cascade on delete cascade
      );

      create unique index idxemail on users(email);

      insert into users values(
        NULL,
        'a@example.org',
        'Alice',
        '5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8',
        0
      );

      insert into users values(
        NULL,
        'b@example.org',
        'Bob',
        '7e0e2d248518efe1cf4cefb953b53665e7a8b1f5c60beea19cc764fddf981d0e',
        0
      );

      insert into friends values(
        1,
        2
      );

      insert into friends values(
        2,
        1
      );

      commit;
    "

actor Main
  new create(env: Env) =>
    try
      let flags = SqliteOpenOptions.readwrite() or
                  SqliteOpenOptions.createdb() or
                  SqliteOpenOptions.memory()

      let db = Sqlite.open(":memory:", flags)?
      db.exec(Schema())?

      let stmt = db.prepare("""
        select email from users where username = ?
      """)?

      stmt.bind(1, "Alice")
      if stmt.next()? then
        let id = stmt.string(0)
        env.out.print("Result is: " + (consume id))
      end
      db.close_stmt(consume stmt)

      let stmt2 = db.prepare("""
        select f.username, f.email
          from users u
          left outer join (friends uu inner join users f on (uu.friendId = f.usersId))
            on (u.usersId = uu.usersId)
         where u.username = ?
      """)?
      stmt2.bind(1, "Bob")
      if stmt2.next()? then
        let username = stmt2.string(0)
        let email = stmt2.string(1)
        env.out.print("Friend is: " + (consume username) + " with email " + (consume email))
      end
      db.close_stmt(consume stmt2)

      Sqlite.close(consume db)
    else
      env.out.print("oops")
    end
