package serialization;

public class MyData {
    private String k1;
    private String k2;
    private K3 k3;

    // No-argument constructor required for deserialization
    public MyData() {}

    // Constructor with parameters
    public MyData(String k1, String k2, K3 k3) {
        this.k1 = k1;
        this.k2 = k2;
        this.k3 = k3;
    }

    // Getters and setters
    public String getK1() { return k1; }
    public void setK1(String k1) { this.k1 = k1; }

    public String getK2() { return k2; }
    public void setK2(String k2) { this.k2 = k2; }

    public K3 getK3() { return k3; }
    public void setK3(K3 k3) { this.k3 = k3; }

    @Override
    public String toString() {
        return "MyData{" + "k1='" + k1 + '\'' + ", k2='" + k2 + '\'' + ", k3=" + k3 + '}';
    }

    // Inner class representing the nested structure
    public static class K3 {
        private String k31;

        // No-argument constructor required for deserialization
        public K3() {}

        // Constructor with parameters
        public K3(String k31) {
            this.k31 = k31;
        }

        public String getK31() { return k31; }
        public void setK31(String k31) { this.k31 = k31; }

        @Override
        public String toString() {
            return "K3{" + "k31='" + k31 + '\'' + '}';
        }
    }
}

