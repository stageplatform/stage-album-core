package pl.pwojcik.stage.album.repository;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.reactivex.Observable;
import io.reactivex.Single;
import jersey.repackaged.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import pl.pwojcik.stage.album.infrastructure.DynamoConnector;
import pl.pwojcik.stage.album.model.Album;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Optional.ofNullable;

/**
 * Class designed for direct communication with persistence layer
 * (database-specific operations).
 * Created by pwojcik on 2017-06-22.
 */
@Repository
public class AlbumRepository {

    private static final Logger LOG = LoggerFactory.getLogger(AlbumRepository.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String DATA_KEY = "objectdata";

    @Autowired
    private DynamoConnector dynamoConnector;

    /**
     * Method responsible for creating or updating Album entities.
     *
     * @param album entity to create/update
     * @return true, if entity has been created/updated
     */
    public boolean storeAlbum(final Album album) {
        try {
            final String json = OBJECT_MAPPER.writeValueAsString(album);
            final Item item = new Item()//
                    .withPrimaryKey(DynamoConnector.ID_KEY, album.getId().toString())//
                    .withJSON(DATA_KEY, json);
            dynamoConnector.getTable().putItem(item);
            return true;
        } catch (final JsonProcessingException exception) {
            LOG.error("Error while serializing Album JSON: {}", album, exception);
            return false;
        }
    }

    /**
     * Method responsible for getting Album entity for given album ID.
     *
     * @param id identifier of the album
     * @return returns Observable with Optional<Album> inside. In case
     * there's no matching data, stream with empty Optional is emitted.
     */
    public Single<Optional<Album>> getAlbum(final String id) {
        final PrimaryKey primaryKey = new PrimaryKey(DynamoConnector.ID_KEY, id);
        final Item item = dynamoConnector.getTable().getItem(primaryKey);
        final Optional<Album> json = ofNullable(item)//
                .map(data -> data.getJSON(DATA_KEY))//
                .map(this::readAlbumEntityFromJSON);
        return Single.just(json);
    }

    private Album readAlbumEntityFromJSON(final String data) {
        try {
            return OBJECT_MAPPER.readValue(data, Album.class);
        } catch (final IOException exception) {
            LOG.error("Error while reading Album JSON stored in database: {}", data, exception);
            return null;
        }
    }

    /**
     * Method responsible for fetching albums collection from database.
     *
     * @return stream of Album objects
     */
    public Observable<Album> getAlbums(final boolean onlyUpcoming) {
        final ScanSpec scanSpecification = createScanSpecification(onlyUpcoming);
        final ItemCollection<ScanOutcome> items = dynamoConnector.getTable().scan(scanSpecification);
        final Collection<Album> results = StreamSupport.stream(items.spliterator(), false)//
                .map(item -> item.getJSON(DATA_KEY))//
                .map(this::readAlbumEntityFromJSON)//
                .collect(Collectors.toList());
        return Observable.fromIterable(results);
    }

    // TODO OnlyReleased - maybe implement QueryParam as enum? or beanParam?
    private ScanSpec createScanSpecification(final boolean onlyUpcoming) {
        if (onlyUpcoming) {
            final String filterExpression = "#a.#b > :val";
            final Map<String, String> nameMap = ImmutableMap.of(//
                    "#a", DATA_KEY,//
                    "#b", "releaseDate"//
            );
            final Map<String, Object> valueMap = ImmutableMap.of(":val", Instant.now().toString());
            return new ScanSpec()//
                    .withFilterExpression(filterExpression)//
                    .withNameMap(nameMap)
                    .withValueMap(valueMap);
        }
        return new ScanSpec();
    }
}
