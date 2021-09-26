package info.kgeorgiy.ja.nesterenko.student;

import info.kgeorgiy.java.advanced.student.AdvancedQuery;
import info.kgeorgiy.java.advanced.student.Group;
import info.kgeorgiy.java.advanced.student.GroupName;
import info.kgeorgiy.java.advanced.student.Student;

import java.util.*;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StudentDB implements AdvancedQuery {
    private static final Comparator<Student> STUDENT_COMPARATOR =
            Comparator.comparing(Student::getLastName)
                    .thenComparing(Student::getFirstName).reversed()
                    .thenComparing(Student::compareTo);

    private static final Comparator<Group> GROUP_NAME_COMPARATOR = Comparator.comparing(Group::getName);

    private <T, R, C extends Collection<R>> C getFieldCollection(Collection<T> collection,
                                                                 Function<T, R> function,
                                                                 Supplier<C> supplier) {
        return collection.stream().map(function).collect(Collectors.toCollection(supplier));
    }

    private <T, R> List<R> getFieldList(Collection<T> collection, Function<T, R> function) {
        return getFieldCollection(collection, function, ArrayList::new);
    }

    private static String getFullName(Student student) {
        return student.getFirstName() + " " + student.getLastName();
    }

    @Override
    public List<String> getFirstNames(List<Student> students) {
        return getFieldList(students, Student::getFirstName);
    }

    @Override
    public List<String> getLastNames(List<Student> students) {
        return getFieldList(students, Student::getLastName);
    }

    @Override
    public List<GroupName> getGroups(List<Student> students) {
        return getFieldList(students, Student::getGroup);
    }

    @Override
    public List<String> getFullNames(List<Student> students) {
        return getFieldList(students, StudentDB::getFullName);
    }

    @Override
    public Set<String> getDistinctFirstNames(List<Student> students) {
        return getFieldCollection(students, Student::getFirstName, TreeSet::new);
    }

    @Override
    public String getMaxStudentFirstName(List<Student> students) {
        return students.stream().max(Student::compareTo).map(Student::getFirstName).orElse("");
    }

    private <T> List<T> sortBy(Collection<T> collection, Comparator<T> comparator) {
        return collection.stream().sorted(comparator).collect(Collectors.toList());
    }

    @Override
    public List<Student> sortStudentsById(Collection<Student> students) {
        return sortBy(students, Student::compareTo);
    }

    @Override
    public List<Student> sortStudentsByName(Collection<Student> students) {
        return sortBy(students, STUDENT_COMPARATOR);
    }

    private <T, R> Stream<T> findByStream(Collection<T> collection, Function<T, R> getter, R key, Comparator<T> comparator) {
        return collection.stream()
                .filter(elem -> getter.apply(elem).equals(key))
                .sorted(comparator);
    }

    private <T, R> List<T> findBy(Collection<T> collection, Function<T, R> getter, R key, Comparator<T> comparator) {
        return findByStream(collection, getter, key, comparator).collect(Collectors.toList());
    }

    private <T> List<Student> findStudentsBy(Collection<Student> students, Function<Student, T> getter, T key) {
        return findBy(students, getter, key, STUDENT_COMPARATOR);
    }

    @Override
    public List<Student> findStudentsByFirstName(Collection<Student> students, String name) {
        return findStudentsBy(students, Student::getFirstName, name);
    }

    @Override
    public List<Student> findStudentsByLastName(Collection<Student> students, String name) {
        return findStudentsBy(students, Student::getLastName, name);
    }

    @Override
    public List<Student> findStudentsByGroup(Collection<Student> students, GroupName group) {
        return findStudentsBy(students, Student::getGroup, group);
    }

    @Override
    public Map<String, String> findStudentNamesByGroup(Collection<Student> students, GroupName group) {
        return findByStream(students, Student::getGroup, group, STUDENT_COMPARATOR)
                .collect(Collectors.toMap(
                        Student::getLastName,
                        Student::getFirstName,
                        BinaryOperator.minBy(String::compareTo)));
    }

    private <T, R> Stream<Map.Entry<T, List<R>>> groupedByStream(Collection<R> collection, Function<R, T> groupingFunction) {
        return collection.stream().collect(Collectors.groupingBy(groupingFunction)).entrySet().stream();
    }

    private List<Group> getGroupsBy(Collection<Student> students,
                                    UnaryOperator<List<Student>> sorter,
                                    Comparator<Group> groupComparator) {
        return groupedByStream(students, Student::getGroup)
                .map(entry -> new Group(entry.getKey(), sorter.apply(entry.getValue())))
                .sorted(groupComparator)
                .collect(Collectors.toList());
    }

    @Override
    public List<Group> getGroupsByName(Collection<Student> students) {
        return getGroupsBy(students, this::sortStudentsByName, GROUP_NAME_COMPARATOR);
    }

    @Override
    public List<Group> getGroupsById(Collection<Student> students) {
        return getGroupsBy(students, this::sortStudentsById, GROUP_NAME_COMPARATOR);
    }

    private <T, R, E> T getMaxStats(Collection<R> collection,
                                    Function<R, T> keyFunction,
                                    Function<R, E> valueFunction,
                                    Comparator<Map.Entry<T, Integer>> comparator,
                                    T defaultValue) {
        return collection.stream()
                .collect(Collectors.groupingBy(
                        keyFunction,
                        Collectors.mapping(valueFunction, Collectors.collectingAndThen(Collectors.toSet(), Set::size))))
                .entrySet().stream()
                .max((Map.Entry.<T, Integer>comparingByValue())
                        .thenComparing(comparator))
                .map(Map.Entry::getKey).orElse(defaultValue);
    }

    @Override
    public GroupName getLargestGroup(Collection<Student> students) {
        return getMaxStats(
                students,
                Student::getGroup,
                Function.identity(),
                Map.Entry.comparingByKey(),
                null);
    }

    @Override
    public GroupName getLargestGroupFirstName(Collection<Student> students) {
        return getMaxStats(
                students,
                Student::getGroup,
                Student::getFirstName,
                Map.Entry.<GroupName, Integer>comparingByKey().reversed(),
                null);
    }

    @Override
    public String getMostPopularName(Collection<Student> students) {
        return getMaxStats(
                students,
                Student::getFirstName,
                Student::getGroup,
                Map.Entry.comparingByKey(),
                ""
        );
    }

    private <T, R> List<R> getByIndicesFromList(List<T> list, Function<T, R> getter, int[] indices) {
        return Arrays.stream(indices).mapToObj(list::get).map(getter).collect(Collectors.toList());
    }

    private <T, R> List<R> getByIndices(Collection<T> collection, Function<T, R> getter, int[] indices) {
        return getByIndicesFromList(new ArrayList<>(collection), getter, indices);
    }

    @Override
    public List<String> getFirstNames(Collection<Student> students, int[] indices) {
        return getByIndices(students, Student::getFirstName, indices);
    }

    @Override
    public List<String> getLastNames(Collection<Student> students, int[] indices) {
        return getByIndices(students, Student::getLastName, indices);
    }

    @Override
    public List<GroupName> getGroups(Collection<Student> students, int[] indices) {
        return getByIndices(students, Student::getGroup, indices);
    }

    @Override
    public List<String> getFullNames(Collection<Student> students, int[] indices) {
        return getByIndices(students, StudentDB::getFullName, indices);
    }
}
