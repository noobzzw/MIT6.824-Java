package util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

public class FileUtil {
    /**
     * 读取文件，并将其转为String
     * @param url 文件路径
     * @return content
     */
    public static String readFileToString(String url) {
        File file = new File(url);
        String content = null;
        try (FileInputStream fs = new FileInputStream(file)) {
            byte[] data = new byte[(int) file.length()];
            fs.read(data);
            content = new String(data, StandardCharsets.UTF_8);
            if (content.length() <= 0) {
                LogUtil.log("Empty File: " + url);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return content;
    }

    /**
     * 获取目录下的所有文件名
     * @return List<FileName>
     */
    public static List<String> readDirFile(String url) {
        File dir = new File(url);
        File[] files = dir.listFiles();
        if (null != files && files.length >= 1) {
            return Arrays.stream(files)
                    .map(File::getName)
                    .collect(Collectors.toList());
        } else {
            return null;
        }
    }

    /**
     * 删除整个文件夹下的文件和子文件夹
     * @param target 路径
     */
    public static void delete(Path target) throws IOException {
        if (Files.isDirectory(target)) {
            // 如果为文件夹，则递归删除所有文件
            Files.walkFileTree(target, EnumSet.of(FileVisitOption.FOLLOW_LINKS), Integer.MAX_VALUE,
                    new SimpleFileVisitor<Path>(){
                        @Override
                        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                            // 遇到文件则直接删除，终止遍历
                            Files.deleteIfExists(file);
                            return super.visitFile(file,attrs);
                        }

                        @Override
                        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                            // 遍历完文件夹后，文件已经全部删除，这时把文件夹也删除
                            Files.deleteIfExists(dir);
                            return super.postVisitDirectory(dir, exc);
                        }
                    });
        } else if (Files.exists(target)){
            Files.delete(target);
        }
    }
}
