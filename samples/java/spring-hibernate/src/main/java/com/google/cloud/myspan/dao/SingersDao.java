package com.google.cloud.myspan.dao;

import com.google.cloud.myspan.entity.Albums;
import com.google.cloud.myspan.entity.Concerts;
import com.google.cloud.myspan.entity.Singers;
import com.google.cloud.myspan.entity.Tracks;
import com.google.cloud.myspan.entity.TracksId;
import com.google.cloud.myspan.entity.Venues;
import java.util.List;
import java.util.UUID;

/**
 *
 * @author Gulam Mustafa
 */
public interface SingersDao {

    List<Singers> getAllSingers();

    Concerts getConcert(UUID uuid);

    Albums getAlbum(UUID uuid);

    Singers getSingers(UUID uuid);

    List<Tracks> getTracks(UUID albumId);

    int updateAlbumMarketingBudget(Albums albums);

    UUID insertSinger(Singers singers);

    TracksId multiObjectsInTransaction(Singers singers, Albums albums, Venues venues, Concerts concerts, Tracks tracks);

    UUID insertAlbum(Albums albums);

    UUID insertConcerts(Concerts concerts);

    TracksId insertTracks(Tracks tracks);

    UUID rollbackTest(Singers singers);

}
