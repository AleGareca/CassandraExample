package dao

import com.datastax.oss.driver.api.core.CqlIdentifier
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.ResultSet
import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.core.cql.SimpleStatement
import com.datastax.oss.driver.api.core.type.DataTypes
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder
import model.Video
import java.util.*
import java.util.function.Consumer

class VideoDaoImpl(private val session: CqlSession) {
    @JvmOverloads
    fun createTable(keyspace: String? = null) {
        val createTable = SchemaBuilder.createTable(TABLE_NAME).ifNotExists()
                .withPartitionKey("video_id", DataTypes.UUID)
                .withColumn("title", DataTypes.TEXT)
                .withColumn("creation_date", DataTypes.TIMESTAMP)
        executeStatement(createTable.build(), keyspace)
    }

    fun insertVideo(video: Video): UUID {
        return insertVideo(video, null)
    }

    fun insertVideo(video: Video, keyspace: String?): UUID {
        val videoId = UUID.randomUUID()
        video.id=videoId
        val insertInto = QueryBuilder.insertInto(TABLE_NAME)
                .value("video_id", QueryBuilder.bindMarker())
                .value("title", QueryBuilder.bindMarker())
                .value("creation_date", QueryBuilder.bindMarker())
        var insertStatement = insertInto.build()
        if (keyspace != null) {
            insertStatement = insertStatement.setKeyspace(keyspace)
        }
        val preparedStatement = session.prepare(insertStatement)
        val statement = preparedStatement.bind()
                .setUuid(0, video.id)
                .setString(1, video.title)
                .setInstant(2, video.creationDate)
        session.execute(statement)
        return videoId
    }


    fun selectAll(keyspace: String?): List<Video> {
        val select = QueryBuilder.selectFrom(TABLE_NAME).all()
        val resultSet = executeStatement(select.build(), keyspace)
        val result: MutableList<Video> = ArrayList()
        resultSet.forEach(Consumer { x: Row ->
            result.add(
                    Video( x.getString("title"), x.getInstant("creation_date"))
            )
        })
        return result
    }

    private fun executeStatement(statement: SimpleStatement, keyspace: String?): ResultSet {
        if (keyspace != null) {
            statement.keyspace = CqlIdentifier.fromCql(keyspace)
        }
        return session.execute(statement)
    }

    companion object {
        private const val TABLE_NAME = "videos"
    }

}
