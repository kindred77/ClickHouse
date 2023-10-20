import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class Main {

    static class AddItem
    {
        public String path;
        public String url;
        public String branch;
        public String ignore;

        public AddItem(String path,String url,String branch,String ignore)
        {
            this.path=path;
            this.url=url;
            this.branch=branch;
            this.ignore=ignore;
        }

        public AddItem()
        {
        }
    }

    private static String convertURL(String originURL) throws Exception
    {
        if(originURL == null || originURL.trim().equals("")) throw new Exception("url is null.");
        originURL = originURL.replaceAll("https://","git@");
        originURL = originURL.replaceFirst("/",":");
        return originURL;
    }

    private static List<AddItem> getAddItems() throws Exception
    {
        List<AddItem> resultList=new ArrayList<>();
        try(BufferedReader br =new BufferedReader(new FileReader(".gitmodules")))
        {
            String str = br.readLine();
            AddItem tmp=null;
            while(str!=null)
            {
                str=str.trim();
                if(str.startsWith("[submodule "))
                {
                    if(tmp!=null)
                    {
                        resultList.add(tmp);
                    }
                    tmp=new AddItem();
                }
                else if(str.startsWith("path = "))
                {
                    tmp.path=str.substring(str.indexOf("path = ")+"path = ".length());
                }
                else if(str.startsWith("url = "))
                {
                    tmp.url=convertURL(str.substring(str.indexOf("url = ")+"url = ".length()));

                }
                else if(str.startsWith("branch = "))
                {
                    tmp.branch=str.substring(str.indexOf("branch = ")+"branch = ".length());
                }
                else if(str.startsWith("ignore = "))
                {
                    tmp.ignore=str.substring(str.indexOf("ignore = ")+"ignore = ".length());
                }
                else
                {
                    throw new Exception("unknown line: "+str);
                }
                str = br.readLine();
            }

            if(tmp!=null)
            {
                resultList.add(tmp);
            }
        }
        return resultList;
    }

    private static void output(List<AddItem> items)  throws Exception
    {
        try(BufferedWriter wr =new BufferedWriter(new FileWriter("add_module.txt")))
        {
            for(AddItem item : items)
            {
                wr.write("git submodule add "+(item.branch==null?"":"-b "+item.branch)+" "+item.url+" "+item.path);
                wr.newLine();
            }
        }
    }

    public static void main(String args[]) throws Exception
    {
        output(getAddItems());
    }
}