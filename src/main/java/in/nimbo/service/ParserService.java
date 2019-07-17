package in.nimbo.service;

import in.nimbo.entity.Page;
import in.nimbo.exception.ParseLinkException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class ParserService {
    private static final int TIMEOUT = 30000;

    public Page parse(String siteLink) {
        List<String> links = new ArrayList<>();
        try {
            Document document = Jsoup.parse(new URL(siteLink), TIMEOUT);
            Elements elements = document.getElementsByTag("a");
            for (Element element : elements) {
                links.add(element.absUrl("href"));
            }
            return new Page(document.html(), links);
        } catch (MalformedURLException e) {
            throw new ParseLinkException("unable to parse url: " + siteLink, e);
        } catch (IOException e) {
            throw new ParseLinkException("unable to parse url with jsoup", e);
        }
    }
}
