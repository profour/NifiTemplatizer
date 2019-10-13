# NifiTemplatizer
The purpose of this project is to create exports of NiFi workspaces that are easier for developers to read and modify. With that purpose in mind, a simplified YAML format was generated to concisely express the content in the NiFi workspace. As an added bonus, the simplified YAML format also makes it easier to perform standard actions (diffs/merges/etc) in version control systems to track changes over time to the NiFi workspace.

# Commands:
* Import (60% complete)
  - Generic
    - Position
    - Styles (Colors/Fonts)
  - Controllers
  - Processors
    - Properties
    - TODO: Advanced rules
  - Ports (Input/Output)
  - Funnels
  - Process Groups
  - Remote Process Groups
  - Labels
    - Comments
    - Styles
  - Connectivity (Inputs/Outputs/Relationships) (90%)
    - TODO: Queueing properties
    - TODO: Links to/from RemoteProcessGroups
* Export (95% complete)
  - Generic
    - Styles (Color/Fonts)
    - Position
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
  - Connectivity (Inputs/Outputs/Relationships)
    - Queue Properties
* Clean (100% complete)
  - Remove all content from workspace (clean slate)
* Set State (100% complete)
  - Enable or disable all processors in a workspace
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
