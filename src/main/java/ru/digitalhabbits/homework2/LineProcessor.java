package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;

public interface LineProcessor<T> {

    @Nonnull
    Pair<String, T> process(@Nonnull String line);
}
