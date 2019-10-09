# NifiTemplatizer
The purpose of this project is to create exports of NiFi workspaces that are easier for humans to read and modify. As an added bonus, the exported yaml files are also easier to track and version in version control systems (such as git). Diffs between versions of the exports are much easier to understand when compared to the normal XML template exports from NiFi, allowing for standard code review processes to be applied to NiFi workspace changes.

# Commands:
* Import (20% complete)
  - Generic
    - Position
  - Controllers
  - Processors
  - Ports (Input/Output)
  - Funnels
  - Process Groups
  - Remote Process Groups
  - Labels
* Export (95% complete)
  - Generic
    - Styles (Color/Fonts)
    - Position
    - Connectivity (Inputs/Outputs/Relationships)
  - Dependency Tree
    - Extract all dependencies of workspace
    - Canonical name generation for dependencies
  - Controllers:
    - Properties
  - Processors
    - Advanced Rules (rules/actions)
    - Properties
    - TODO: Scheduling settings
  - Ports (Input/Output)
  - Funnels
  - Process Groups
    - Export content as sub-template
    - TODO: De-duplicate structurally similar templates (extract variables for varying properties)
  - Remote Process Groups
    - TODO: Properties of Remote Ports
  - Labels
    - Width/Height
    - Text content
* Clean (0% complete)
  - TODO: Remove all content from workspace (clean slate)
* Permissions (0% complete)
  - TODO: Give all permissions to specified user

# Example
Test workspace to cover lots of commonly used features in NiFi. The exported YAML is roughly 1/10th the size of the XML template export.

Total Size: 
- YAML Export = 5297 bytes (8% size of XML)
- XML  Export = 66842 bytes

[Export of the NiFi template XML](https://github.com/profour/NifiTemplatizer/blob/master/examples/simple/Simple_Example.xml)

[Export of the Root workspace](https://github.com/profour/NifiTemplatizer/blob/master/examples/simple/root.yaml)

[Export of the Test Subgroup](https://github.com/profour/NifiTemplatizer/blob/master/examples/simple/bbfb5e15-016c-1000-24e9-c7827e34b838.yaml)


Input Root workspace:
![](examples/simple/root.png)

Test Process Group workspace:
![](examples/simple/subprocessgroup.png)
