package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LineCounterProcessorTest {

    @ParameterizedTest
    @MethodSource("testDataGenerator")
    void test(Pair<String, Integer> testData) {
        final Pair<String, Integer> result = new LineCounterProcessor().process(testData.getKey());
        assertEquals(testData.getValue(), result.getValue());
    }

    static Stream<Pair<String, Integer>> testDataGenerator() {
        return Stream.of(
                Pair.of("Hello, world", 12),
                Pair.of("Alex", 4),
                Pair.of("I'll be back", 12),
                Pair.of("Foo Bar", 7)
        );
    }
}