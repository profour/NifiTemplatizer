# NifiTemplatizer
The purpose of this project is to create exports of NiFi workspaces that are easier for humans to read and modify. As an added bonus, the exported yaml files are also easier to track and version in version control systems (such as git). Diffs between versions of the exports are much easier to understand when compared to the normal XML template exports from NiFi, allowing for standard code review processes to be applied to NiFi workspace changes.

# Example
Test workspace to cover lots of commonly used features in NiFi. The exported YAML is roughly 1/10th the size of the XML template export.

Total Size: 
- YAML Export = 5297 bytes (8% size of XML)
- XML  Export = 66842 bytes

[Export of the Root workspace](https://github.com/profour/NifiTemplatizer/blob/master/examples/simple/root.yaml)

[Export of the Test Subgroup](https://github.com/profour/NifiTemplatizer/blob/master/examples/simple/bbfb5e15-016c-1000-24e9-c7827e34b838.yaml)


Input Root workspace:
![](examples/simple/root.png)

Test Process Group workspace:
![](examples/simple/subprocessgroup.png)
