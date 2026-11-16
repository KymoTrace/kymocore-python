"""Data-only ELF/DWARF symbol maps for Studio's existing variable browser.

The file describes virtual Watch addresses. It contains no executable code or
process memory. Its SHA-256 prefix identifies the exact exported layout.
"""
import struct


def symbol_elf(variables, base_address, build_tag):
    """Build deterministic ELF32 LE with scalar DWARF v4 types (stdlib only)."""
    # Abbreviations: compilation unit (children), base type, global variable.
    # DW_FORM_string=8, data1=11, ref4=19, exprloc=24. All codes fit ULEB128.
    abbrev = bytes((
        1, 0x11, 1, 0x03, 8, 0x25, 8, 0, 0,
        2, 0x24, 0, 0x03, 8, 0x3e, 11, 0x0b, 11, 0, 0,
        3, 0x34, 0, 0x03, 8, 0x49, 19, 0x02, 24, 0, 0,
        0,
    ))
    # Unit offsets include the initial four-byte unit-length field.
    debug = bytearray(struct.pack('<HIB', 4, 0, 4))
    debug.extend(b'\x01python-watch-map\0kymocore-python/' + build_tag.encode('utf-8') + b'\0')
    type_offsets = {}
    types = ((1, 'u8', 7, 1), (2, 'i8', 5, 1), (3, 'u16', 7, 2), (4, 'i16', 5, 2),
             (5, 'u32', 7, 4), (6, 'i32', 5, 4), (7, 'f32', 4, 4), (8, 'f64', 4, 8), (9, 'bool', 2, 1))
    sizes = {}
    for code, name, encoding, size in types:
        type_offsets[code] = len(debug) + 4
        sizes[code] = size
        debug.extend(b'\x02' + name.encode('ascii') + bytes((0, encoding, size)))
    strings = bytearray(b'\0')
    symbols = bytearray(16)  # mandatory null symbol
    for index, variable in enumerate(variables):
        address = base_address + index * 8
        name = variable.name.encode('utf-8')
        offset = len(strings)
        strings.extend(name + b'\0')
        # STB_GLOBAL|STT_OBJECT; section 1 is our non-file-backed virtual data.
        symbols.extend(struct.pack('<IIIBBH', offset, address, sizes[variable.type], 0x11, 0, 1))
        debug.extend(b'\x03' + name + b'\0' + struct.pack('<I', type_offsets[variable.type])
                     + b'\x05\x03' + struct.pack('<I', address))
    debug.append(0)  # compilation-unit children end
    debug = struct.pack('<I', len(debug)) + debug
    section_names = b'\0.bss\0.symtab\0.strtab\0.debug_abbrev\0.debug_info\0.shstrtab\0'
    data = bytearray(52)
    sections = [bytes(40)]

    def section(name, kind, payload, *, flags=0, address=0, link=0, info=0, alignment=1, entry_size=0, size=None):
        data.extend(b'\0' * (-len(data) % alignment))
        offset = len(data)
        if kind != 8:  # SHT_NOBITS occupies no file bytes
            data.extend(payload)
        sections.append(struct.pack('<IIIIIIIIII', section_names.index(name.encode('ascii') + b'\0'),
            kind, flags, address, offset, len(payload) if size is None else size,
            link, info, alignment, entry_size))

    section('.bss', 8, b'', flags=3, address=base_address, alignment=8, size=len(variables) * 8)
    section('.symtab', 2, symbols, link=3, info=1, alignment=4, entry_size=16)
    section('.strtab', 3, strings)
    section('.debug_abbrev', 1, abbrev)
    section('.debug_info', 1, debug)
    section('.shstrtab', 3, section_names)
    data.extend(b'\0' * (-len(data) % 4))
    section_offset = len(data)
    data.extend(b''.join(sections))
    identification = b'\x7fELF\x01\x01\x01' + b'\0' * 9
    # EM_NONE: architecture-independent symbol map; no program headers/entry.
    data[:52] = identification + struct.pack('<HHIIIIIHHHHHH', 2, 0, 1, 0, 0,
        section_offset, 0, 52, 0, 0, 40, len(sections), 6)
    return bytes(data)
