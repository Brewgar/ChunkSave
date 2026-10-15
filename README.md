# ChunkSave

A Fabric 1.20.1 mod that completely prevents out of memory crashes without almost any performance cost when pre-generating massive world generation mods like [JJThunder](https://modrinth.com/datapack/jjthunder-to-the-max) or [Big Globe](https://modrinth.com/mod/big-globe) with tools like [Chunky](https://github.com/pop4959/Chunky). 

## The Problem
When you pre-generate a Minecraft world, the game often creates new chunks much faster than your drive can save them. Vanilla Minecraft handles this by keeping all those unsaved chunks in your RAM. If your drive can't keep up with the generation speed, your RAM eventually fills up completely, causing the game or server to crash.

## The Solution
ChunkSave fixes this by acting as a traffic controller between world generation and your hard drive. 

Instead of letting RAM fill up infinitely, ChunkSave sets a strict limit on how many chunks can wait in memory. If that limit is reached, it temporarily pauses world generation until your drive catches up. 

**To Install:**
Drop the `.jar` file into your `mods` folder. 
*Requires [Fabric Loader](https://fabricmc.net/use/installer/) 0.15 or higher and the [Fabric API](https://modrinth.com/mod/fabric-api).*

**To Build:**
Make sure you have JDK 17 installed. Run `gradle build` in the project directory. The compiled mod will be located in `build/libs/`.
