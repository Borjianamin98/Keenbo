package in.nimbo.entity;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Page {
    @JsonProperty("link")
    private String link;
    @JsonProperty("title")
    private String title;
    @JsonProperty("content")
    private String content;

    public Page() {
    }

    public Page(String link, String title, String content) {
        this.title = title;
        this.content = content;
        this.link = link;
    }

    public String getTitle() {
        return title;
    }

    public String getLink() {
        return link;
    }

    public String getContent() {
        return content;
    }

    public void setLink(String link) {
        this.link = link;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
