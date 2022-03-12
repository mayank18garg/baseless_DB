package diskmgr;

import btree.KeyClass;
import btree.KeyDataEntry;
import btree.StringKey;
import global.AttrType;
import global.EID;
import global.GlobalConst;
import global.LID;
import quadrupleheap.QuadrupleHeapfile;

public class rdfDB_P extends DB implements GlobalConst {

    private QuadrupleHeapfile tempQuadrapleHF;        //Temporary heap file for sorting
    private QuadrupleHeapfile quadrapleHF;            //Quadraple heap file to store the quadraples
    private LabelHeapFile entityHF;                   //Entity heap file to store subjects/objects
    private LabelHeapFile predicateHF;                //Predicate heap file to store predicates

    private LabelBTreeFile entityBTree;
    private LabelBTreeFile predicateBTree;

    private String current_dbname;

    /**
     * Insert a entity into the EntityHeapFIle
     * @param Entitylabel String representing Subject/Object
     */
    public EID insertEntity(String EntityLabel) {
        // int KeyType = AttrType.attrString;
        KeyClass key = new StringKey(EntityLabel);
        EID entityid = null;

        //Open ENTITY BTree Index file
        try {
            EntityBTree = new LabelBTreeFile(current_dbname + "/entityBT");
            //      LabelBT.printAllLeafPages(Entity_BTree.getHeaderPage());

            LID lid = null;
            KeyClass low_key = new StringKey(EntityLabel);
            KeyClass high_key = new StringKey(EntityLabel);
            KeyDataEntry entry = null;

            //Start Scaning Btree to check if entity already present
            LabelBTFileScan scan = Entity_BTree.new_scan(low_key, high_key);
            entry = scan.get_next();
            if (entry != null) {
                if (EntityLabel.equals(((StringKey) (entry.key)).getKey())) {
                    //return already existing EID ( convert lid to EID)
                    lid = ((LabelLeafData) entry.data).getData();
                    entityid = lid.returnEID();
                    scan.DestroyBTreeFileScan();
                    Entity_BTree.close();
                    return entityid;
                }
            }

            scan.DestroyBTreeFileScan();
            //Insert into Entity HeapFile
            lid = Entity_HF.insertRecord(EntityLabel.getBytes());

            //Insert into Entity Btree file key,lid
            Entity_BTree.insert(key, lid);

            entityid = lid.returnEID();
            Entity_BTree.close();
        } catch (Exception e) {
            System.err.println("*** Error inserting entity ");
            e.printStackTrace();
        }

        return entityid; //Return EID
    }

}