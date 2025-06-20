class Doktl {
    
    private String dokVersion;
    private String id;
    private String langu;
    private String line;
    private String object;
    private String typ;

    public Doktl(String dokVersion, String id, String langu, String line, String object, String typ) {
        this.dokVersion = dokVersion;
        this.id = id;
        this.langu = langu;
        this.line = line;
        this.object = object;
        this.typ = typ;
    }

    // Getters and Setters
    public String getDokVersion() {
        return dokVersion;
    }

    public void setDokVersion(String dokVersion) {
        this.dokVersion = dokVersion;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getLangu() {
        return langu;
    }

    public void setLangu(String langu) {
        this.langu = langu;
    }

    public String getLine() {
        return line;
    }

    public void setLine(String line) {
        this.line = line;
    }

    public String getObject() {
        return object;
    }

    public void setObject(String object) {
        this.object = object;
    }

    public String getTyp() {
        return typ;
    }

    public void setTyp(String typ) {
        this.typ = typ;
    }   
}