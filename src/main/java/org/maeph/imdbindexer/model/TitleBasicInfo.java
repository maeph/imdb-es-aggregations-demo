package org.maeph.imdbindexer.model;


import lombok.*;


@Getter
@AllArgsConstructor
@EqualsAndHashCode
@ToString
@Builder
public class TitleBasicInfo {

    String tconst;
    String titleType;
    String primaryTitle;
    String originalTitle;
    Boolean isAdult;
    Integer startYear;
    Integer endYear;
    Integer runtimeMinutes;
    String[] genres;
}
