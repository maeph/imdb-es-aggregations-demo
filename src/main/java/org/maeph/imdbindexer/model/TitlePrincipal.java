package org.maeph.imdbindexer.model;


import lombok.*;

@Getter
@AllArgsConstructor
@EqualsAndHashCode
@ToString
@Builder
public class TitlePrincipal {
    String movieTitle;
    Integer ordering;
    String name;
    String category;
    String job;
    String[] characters;
}
