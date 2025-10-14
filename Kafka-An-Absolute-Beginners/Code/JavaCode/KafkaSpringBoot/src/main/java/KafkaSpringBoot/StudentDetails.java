package KafkaSpringBoot;

public class StudentDetails {
    private String name;
    private int age;

    // Constructors, Getters, and Setters
    public StudentDetails() {}
    
    public StudentDetails(String name, int age) {
        this.name = name;
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }
}
