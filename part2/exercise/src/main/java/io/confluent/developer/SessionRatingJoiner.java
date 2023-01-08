package io.confluent.developer;

import org.apache.kafka.streams.kstream.ValueJoiner;

import io.confluent.developer.avro.Session;
import io.confluent.developer.avro.RatedSession;
import io.confluent.developer.avro.Rating;

public class SessionRatingJoiner implements ValueJoiner<Rating, Session, RatedSession> {

  public RatedSession apply(Rating rating, Session session) {
    return RatedSession.newBuilder()
        .setId(session.getId())
        .setTitle(session.getTitle())
        .setPresenter(session.getPresenter())
        .setRating(rating.getRating())
        .build();
  }
}

