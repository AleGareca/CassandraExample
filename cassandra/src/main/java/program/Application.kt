package program

import com.datastax.oss.driver.api.core.CqlSession
import dao.KeyspaceRepository
import dao.VideoDaoImpl
import model.Video
import org.slf4j.LoggerFactory
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.function.Consumer


class Application {
    fun run() {
        val connector = CassandraConnector()
        connector.connect("127.0.0.1", 9042, "datacenter1")
        val session: CqlSession = connector.session!!
        val keyspaceRepository = KeyspaceRepository(session)
        keyspaceRepository.createKeyspace("testKeyspace", 1)
        keyspaceRepository.useKeyspace("testKeyspace")
        val videoRepository = VideoDaoImpl(session)
        videoRepository.createTable()
        videoRepository.insertVideo(Video( "Video Title 1", Instant.now()))
        videoRepository.insertVideo(Video(
                "Video Title 2",
                Instant.now().minus(1, ChronoUnit.DAYS)))
        val videos: List<Video> = videoRepository.selectAll("testKeyspace")
        videos.forEach(Consumer<Video> { x: Video -> LOG.info(x.toString()) })
        connector.close()
    }

    companion object {
        private val LOG = LoggerFactory.getLogger(Application::class.java)

        @JvmStatic
        fun main(args: Array<String>) {
            var ap= Application()
            ap.run()
        }
    }
}
