package pl.pwojcik.stage.album.service;

import io.reactivex.Observable;
import io.reactivex.Single;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import pl.pwojcik.stage.album.model.Album;
import pl.pwojcik.stage.album.repository.AlbumRepository;

import java.util.Optional;

/**
 * Created by pwojcik on 2017-06-22.
 */
@Service
public class AlbumService {

    @Autowired
    private AlbumRepository albumRepository;

    /**
     * Method responsible for fetching Album by albumID.
     *
     * @param albumId identifier of album
     * @return Response with HTTP Status Code (and entity if possible).
     */
    public Single<Optional<Album>> getAlbum(final String albumId) {
        return albumRepository.getAlbum(albumId);
    }

    /**
     * Method responsible for fetching collection of Albums.
     *
     * @param onlyUpcoming if true, already released albums will be filtered out
     * @return Observable stream of Albums
     */
    public Observable<Album> getAlbums(final boolean onlyUpcoming) {
        return albumRepository.getAlbums(onlyUpcoming);
    }

    /**
     * Method responsible for creating or updating Album entities.
     *
     * @param album entity to create/update
     * @return true, if entity has been created/updated
     */
    public boolean storeAlbum(final Album album) {
        // TODO check if album is valid and is not null (stream npe on just)
        return albumRepository.storeAlbum(album);
    }
}
