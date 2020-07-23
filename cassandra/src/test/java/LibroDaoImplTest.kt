import com.datastax.driver.core.ColumnDefinitions.Definition
import com.datastax.driver.core.Session
import com.datastax.driver.core.exceptions.InvalidQueryException
import com.datastax.driver.core.utils.UUIDs
import dao.KeyspaceRepositoryLibro
import dao.LibroDaoImpl
import junit.framework.Assert.assertEquals
import model.Libro
import org.apache.cassandra.exceptions.ConfigurationException
import org.apache.thrift.transport.TTransportException
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.junit.*
import program.libro.CassandraConnector

import java.io.IOException
import java.util.stream.Collectors


class LibroDaoImplTest {
    private var schemaRepository: KeyspaceRepositoryLibro? = null

     lateinit var libroDaoImpl: LibroDaoImpl

    private var session: Session? = null

    val KEYSPACE_NAME = "testLibrary"
    val BOOKS = "books"
    val BOOKS_BY_TITLE = "booksByTitle"

    @Before
    @Throws(ConfigurationException::class, TTransportException::class, IOException::class, InterruptedException::class)
    fun init() {
        // Start an embedded Cassandra Server
        EmbeddedCassandraServerHelper.startEmbeddedCassandra(20000L)
    }

    @Before
    fun connect() {
        val client = CassandraConnector()
        client.connect("127.0.0.1", 9042)
        session = client.getSession()
        schemaRepository = KeyspaceRepositoryLibro(session!!)
        schemaRepository!!.createKeyspace(KEYSPACE_NAME, "SimpleStrategy", 1)
        schemaRepository!!.useKeyspace(KEYSPACE_NAME)
        libroDaoImpl = LibroDaoImpl(session)
    }

    @Test
    fun whenCreatingATable_thenCreatedCorrectly() {
        libroDaoImpl.deleteTable(BOOKS)
        libroDaoImpl.createTable()
        val result = session!!.execute("SELECT * FROM $KEYSPACE_NAME.$BOOKS;")

        // Collect all the column names in one list.
        val columnNames = result.columnDefinitions.asList().stream().map { cl: Definition -> cl.name }.collect(Collectors.toList())
        Assert.assertEquals(columnNames.size.toLong(), 4)
        Assert.assertTrue(columnNames.contains("id"))
        Assert.assertTrue(columnNames.contains("title"))
        Assert.assertTrue(columnNames.contains("author"))
        Assert.assertTrue(columnNames.contains("subject"))
    }

    @Test
    fun whenAlteringTable_thenAddedColumnExists() {
        libroDaoImpl.deleteTable(BOOKS)
        libroDaoImpl.createTable()
        libroDaoImpl.alterTablebooks("publisher", "text")
        val result = session!!.execute("SELECT * FROM $KEYSPACE_NAME.$BOOKS;")
        val columnExists = result.columnDefinitions.asList().stream().anyMatch { cl: Definition -> cl.name == "publisher" }
        Assert.assertTrue(columnExists)
    }

    @Test
    fun whenAddingANewBook_thenBookExists() {
        libroDaoImpl.deleteTable(BOOKS_BY_TITLE)
        libroDaoImpl.createTableBooksByTitle()
        val title = "Effective Java"
        val author = "Joshua Bloch"
        val libro = Libro(UUIDs.timeBased(), title,author, "Programming","")
        libroDaoImpl.insertbookByTitle(libro)
        val savedBook = libroDaoImpl.selectByTitle(title)
        assertEquals(libro.title, savedBook!!.title)
    }

    @Test
    fun whenAddingANewBookBatch_ThenBookAddedInAllTables() {
        // Create table books
        libroDaoImpl.deleteTable(BOOKS)
        libroDaoImpl.createTable()

        // Create table booksByTitle
        libroDaoImpl.deleteTable(BOOKS_BY_TITLE)
        val author = "Joshua Bloch"
        libroDaoImpl.createTableBooksByTitle()
        val title = "Effective Java"
        val book = Libro(UUIDs.timeBased(), title, "Programming","","")
        libroDaoImpl.insertBookBatch(book)
        val libros = libroDaoImpl.selectAll()
        Assert.assertEquals(1, libros!!.size.toLong())
        Assert.assertTrue(libros!!.stream().anyMatch { b: Libro -> b.title.equals("Effective Java") })
        val librosPorTitulos= libroDaoImpl.selectAllBookByTitle()
        Assert.assertEquals(1, librosPorTitulos!!.size.toLong())
        Assert.assertTrue(librosPorTitulos.stream().anyMatch { b: Libro -> b.title.equals("Effective Java") })
    }

    @Test
    fun whenSelectingAll_thenReturnAllRecords() {
        libroDaoImpl.deleteTable(BOOKS)
        libroDaoImpl.createTable()
        var libro = Libro(UUIDs.timeBased(), "Effective Java", "Programming","","")
        libroDaoImpl.insertbook(libro!!)
        libro = Libro(UUIDs.timeBased(), "Clean Code", "Programming","","")
        libroDaoImpl.insertbook(libro)
        val libros = libroDaoImpl.selectAll()
        Assert.assertEquals(2, libros!!.size.toLong())
        Assert.assertTrue(libros!!.stream().anyMatch { b: Libro -> b.title.equals("Effective Java") })
        Assert.assertTrue(libros!!.stream().anyMatch { b: Libro -> b.title.equals("Clean Code") })
    }

    @Test
    fun whenDeletingABookByTitle_thenBookIsDeleted() {
        libroDaoImpl.deleteTable(BOOKS_BY_TITLE)
        libroDaoImpl.createTableBooksByTitle()
        var libro: Libro? = Libro(UUIDs.timeBased(), "Effective Java", "Programming","","")
        libroDaoImpl.insertbookByTitle(libro!!)
        libro = Libro(UUIDs.timeBased(), "Clean Code", "Programming","","")
        libroDaoImpl.insertbookByTitle(libro)
        libroDaoImpl.deletebookByTitle("Clean Code")
        val libros = libroDaoImpl.selectAllBookByTitle()
        Assert.assertEquals(1, libros!!.size.toLong())
        Assert.assertTrue(libros!!.stream().anyMatch { b: Libro -> b.title.equals("Effective Java") })
        Assert.assertFalse(libros!!.stream().anyMatch { b: Libro -> b.title.equals("Clean Code") })
    }

    @Test(expected = InvalidQueryException::class)
    fun whenDeletingATable_thenUnconfiguredTable() {
        libroDaoImpl.createTable()
        libroDaoImpl.deleteTable(BOOKS)
        session!!.execute("SELECT * FROM $KEYSPACE_NAME.$BOOKS;")
    }
/*
    @After
    fun cleanup() {
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
    }*/
}