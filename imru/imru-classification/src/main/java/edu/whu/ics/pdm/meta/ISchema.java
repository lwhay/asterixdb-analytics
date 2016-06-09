/**
 * 
 */
package edu.whu.ics.pdm.meta;

import java.util.List;

import edu.whu.ics.pdm.algconf.IOption;

/**
 * @author michael
 *
 */
public interface ISchema {
    public String globalInfo();

    public List<IOption> getOptions();

    public void addOptions(List<IOption> options);

    public void addOption(IOption option);

    public void deleteOption(IOption option);

    public void setFolder(int num);

    public int getFolder();
}
