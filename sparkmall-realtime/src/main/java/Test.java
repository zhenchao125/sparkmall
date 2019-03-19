public class Test {
    public static void main(String[] args) {
        Person p1 = new Person();
        Person p2 = new Person();
        p1.setName("lisi");
        p2.setName("zs");

        p1.play();
        p2.play();

        p1.foo();
        p1.getName();
    }
}

class Person{
    private String name;  // 属性  成员变量

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void play(){
        String name = "lisi";
        System.out.println(this.name);
    }

    public void foo(){
        this.play();  // === this.play()
    }
}
