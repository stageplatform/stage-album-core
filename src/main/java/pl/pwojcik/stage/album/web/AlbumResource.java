package pl.pwojcik.stage.album.web;

import io.reactivex.Observable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.request.async.DeferredResult;
import pl.pwojcik.stage.album.model.Album;
import pl.pwojcik.stage.album.service.AlbumService;
import pl.pwojcik.stage.security.access.AccessInspector;

import javax.ws.rs.core.MediaType;
import java.util.List;
import java.util.Optional;

/**
 * Created by pwojcik on 2017-06-20.
 */
@RestController
@RequestMapping("/album")
public class AlbumResource {

    private static final Logger LOG = LoggerFactory.getLogger(AlbumResource.class);

    @Autowired
    private AlbumService albumService;

    @Value("${album.endpoint.timeout}")
    private long timeout;

    @RequestMapping(method = RequestMethod.GET, value = "/{id}", produces = MediaType.APPLICATION_JSON)
    public DeferredResult<ResponseEntity> getAlbum(@PathVariable("id") final String albumId) {
        AccessInspector.verifyUUID(albumId);
        final DeferredResult<ResponseEntity> deferred = new DeferredResult<>(timeout);
        albumService.getAlbum(albumId)//
                .map(this::verifyAlbumData)//
                .onErrorReturn(error -> provideResponseOnAlbumFetchError(albumId, error))//
                .subscribe(deferred::setResult, deferred::setErrorResult);
        return deferred;
    }

    private ResponseEntity verifyAlbumData(final Optional<Album> album) {
        return album.map(ResponseEntity::ok)//
                .orElse(ResponseEntity.notFound().build());
    }

    private ResponseEntity provideResponseOnAlbumFetchError(final String albumId, final Throwable error) {
        LOG.error("Error while fetching details of album with ID: {} ", albumId, error);
        return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @RequestMapping(method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON)
    public DeferredResult<ResponseEntity> getAlbums(@RequestParam(value = "onlyUpcoming", required = false, //
            defaultValue = "false") final boolean onlyUpcoming) {
        final DeferredResult<ResponseEntity> deferred = new DeferredResult<>(timeout);
        albumService.getAlbums(onlyUpcoming).toList()//
                .map(this::verifyAlbumData)//
                .onErrorReturn(error -> provideResponseOnAlbumListFetchError(error))//
                .subscribe(deferred::setResult, deferred::setErrorResult);
        return deferred;
    }

    private ResponseEntity verifyAlbumData(final List<Album> albums) {
        return albums.isEmpty() ? ResponseEntity.notFound().build() : ResponseEntity.ok(albums);
    }

    private ResponseEntity provideResponseOnAlbumListFetchError(final Throwable error) {
        LOG.error("Error while fetching albums.", error);
        return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @RequestMapping(method = RequestMethod.PUT, consumes = MediaType.APPLICATION_JSON)
    public DeferredResult<ResponseEntity> storeAlbum(@RequestBody final Album album) {
        final DeferredResult<ResponseEntity> deferred = new DeferredResult<>(timeout);
        Observable.just(albumService.storeAlbum(album))//
                .map(this::verifyStorageResponse)//
                .subscribe(deferred::setResult, deferred::setErrorResult);
        return deferred;
    }

    private ResponseEntity<Object> verifyStorageResponse(final boolean stored) {
        if (stored) {
            return ResponseEntity.status(HttpStatus.CREATED).build();
        }
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
    }
}
