package dao

import com.datastax.driver.core.Session
import model.Libro
import java.util.*

class LibroDaoImpl( session: Session?) {
    private val TABLE_NAME = "books"

    private val TABLE_NAME_BY_TITLE = TABLE_NAME + "ByTitle"

    private var session: Session? = session


    /**
     * Creates the books table.
     */
    fun createTable() {
        val sb = StringBuilder("CREATE TABLE IF NOT EXISTS ").append(TABLE_NAME).append("(").append("id uuid PRIMARY KEY, ").append("title text,").append("author text,").append("subject text);")
        val query = sb.toString()
        session!!.execute(query)
    }

    /**
     * Creates the books table.
     */
    fun createTableBooksByTitle() {
        val sb = StringBuilder("CREATE TABLE IF NOT EXISTS ").append(TABLE_NAME_BY_TITLE).append("(").append("id uuid, ").append("title text,").append("PRIMARY KEY (title, id));")
        val query = sb.toString()
        session!!.execute(query)
    }

    /**
     * Alters the table books and adds an extra column.
     */
    fun alterTablebooks(columnName: String?, columnType: String?) {
        val sb = StringBuilder("ALTER TABLE ").append(TABLE_NAME).append(" ADD ").append(columnName).append(" ").append(columnType).append(";")
        val query = sb.toString()
        session!!.execute(query)
    }

    /**
     * Insert a row in the table books.
     *
     * @param book
     */
    fun insertbook(libro: Libro) {
        val sb: StringBuilder = StringBuilder("INSERT INTO ").append(TABLE_NAME).append("(id, title, author, subject) ").append("VALUES (").append(libro.id).append(", '").append(libro.title).append("', '").append(libro.author).append("', '")
                .append(libro.subject).append("');")
        val query = sb.toString()
        session!!.execute(query)
    }

    /**
     * Insert a row in the table booksByTitle.
     * @param libro
     */
    fun insertbookByTitle(libro: Libro) {
        val sb: StringBuilder = StringBuilder("INSERT INTO ").append(TABLE_NAME_BY_TITLE).append("(id, title) ").append("VALUES (").append(libro.id).append(", '").append(libro.title).append("');")
        val query = sb.toString()
        session!!.execute(query)
    }

    /**
     * Insert a book into two identical tables using a batch query.
     *
     * @param book
     */
    fun insertBookBatch(libro: Libro) {
        val sb: StringBuilder = StringBuilder("BEGIN BATCH ").append("INSERT INTO ").append(TABLE_NAME).append("(id, title, author, subject) ").append("VALUES (").append(libro.id).append(", '").append(libro.title).append("', '").append(libro.author)
                .append("', '").append(libro.subject).append("');").append("INSERT INTO ").append(TABLE_NAME_BY_TITLE).append("(id, title) ").append("VALUES (").append(libro.id).append(", '").append(libro.title).append("');")
                .append("APPLY BATCH;")
        val query = sb.toString()
        session!!.execute(query)
    }

    /**
     * Select book by id.
     *
     * @return
     */
    fun selectByTitle(title: String?): Libro? {
        val sb = StringBuilder("SELECT * FROM ").append(TABLE_NAME_BY_TITLE).append(" WHERE title = '").append(title).append("';")
        val query = sb.toString()
        val rs = session!!.execute(query)
        val books: MutableList<Libro> = ArrayList()
        for (r in rs) {
            val s = Libro(r.getUUID("id"), r.getString("title"), "","","")
            books.add(s)
        }
        return books[0]
    }

    /**
     * Select all books from books
     *
     * @return
     */
    fun selectAll(): List<Libro>? {
        val sb = StringBuilder("SELECT * FROM ").append(TABLE_NAME)
        val query = sb.toString()
        val rs =session!!.execute(query)
        val books: MutableList<Libro> = ArrayList<Libro>()
        for (r in rs) {
            val book = Libro(r.getUUID("id"), r.getString("title"), r.getString("subject"),"","")
            books.add(book)
        }
        return books
    }

    /**
     * Select all books from booksByTitle
     * @return
     */
    fun selectAllBookByTitle(): List<Libro>? {
        val sb = StringBuilder("SELECT * FROM ").append(TABLE_NAME_BY_TITLE)
        val query = sb.toString()
        val rs = session!!.execute(query)
        val books: MutableList<Libro> = ArrayList<Libro>()
        for (r in rs) {
            val book = Libro(r.getUUID("id"), r.getString("title"),"","","")
            books.add(book)
        }
        return books
    }

    /**
     * Delete a book by title.
     */
    fun deletebookByTitle(title: String?) {
        val sb = StringBuilder("DELETE FROM ").append(TABLE_NAME_BY_TITLE).append(" WHERE title = '").append(title).append("';")
        val query = sb.toString()
        session!!.execute(query)
    }

    /**
     * Delete table.
     *
     * @param tableName the name of the table to delete.
     */
    fun deleteTable(tableName: String?) {
        val sb = StringBuilder("DROP TABLE IF EXISTS ").append(tableName)
        val query = sb.toString()
        session!!.execute(query)
    }
}